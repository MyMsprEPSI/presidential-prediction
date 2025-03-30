# loader.py

import logging
import os
import shutil
import pandas as pd
from typing import Optional

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Classe permettant d'enregistrer un DataFrame PySpark transformé en fichier CSV.
    Le fichier est nommé selon le format "<nom_fichier_base>_processed.csv".
    """

    def __init__(self, spark, output_dir="data\processed_data"):
        """
        Initialise le DataLoader avec un répertoire de sortie.

        :param output_dir: Dossier où seront stockés les fichiers CSV transformés.
        """
        logger.info(
            f"🚀 Initialisation du DataLoader avec le dossier de sortie : {output_dir}"
        )
        self.spark = spark
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas
        logger.info("✅ Dossier de sortie créé/validé")

    def save_to_csv(self, df, input_file_path):
        """
        Sauvegarde un DataFrame en fichier CSV après transformation.
        Compatible avec les DataFrames pandas et Spark.
        """
        if df is None:
            logger.error("❌ Impossible de sauvegarder un DataFrame vide.")
            return

        # Normaliser le chemin d'entrée
        input_file_path = os.path.normpath(input_file_path)
        base_name = os.path.basename(input_file_path).replace(".csv", "_processed.csv")
        final_output_path = os.path.normpath(os.path.join(self.output_dir, base_name))

        logger.info(
            f"⚡ Enregistrement des données transformées dans : {final_output_path}"
        )

        try:
            # Vérifier si c'est un DataFrame pandas ou Spark
            if hasattr(df, "toPandas"):  # C'est un DataFrame Spark
                # Utiliser la méthode originale pour Spark
                df = df.coalesce(1)
                df.write.mode("overwrite").option("header", "true").option(
                    "delimiter", ";"  # Utiliser la virgule comme séparateur
                ).csv(final_output_path + "_temp")

                if temp_file := next(
                    (
                        os.path.join(final_output_path + "_temp", filename)
                        for filename in os.listdir(final_output_path + "_temp")
                        if filename.endswith(".csv")
                    ),
                    None,
                ):
                    shutil.copy2(temp_file, final_output_path)
                    shutil.rmtree(final_output_path + "_temp")
                    logger.info("✅ Fichier CSV sauvegardé avec succès !")
                else:
                    logger.error(
                        "❌ Aucun fichier CSV généré dans le dossier temporaire."
                    )
            else:  # C'est un DataFrame pandas
                # Pour le fichier de sécurité, on utilise toujours le header
                df.to_csv(final_output_path, sep=";", index=False, header=True)
                logger.info("✅ Fichier CSV sauvegardé avec succès via pandas!")

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement du fichier : {str(e)}")
            if os.path.exists(final_output_path + "_temp"):
                shutil.rmtree(final_output_path + "_temp")
                
    def generate_consolidated_csv_from_files(
        self,
        election_csv,    # Chemin vers le CSV des données politiques
        security_csv,    # Chemin vers le CSV de la sécurité
        socio_csv,       # Chemin vers le CSV de la socio-économie
        sante_csv,       # Chemin vers le CSV de la santé
        env_csv,         # Chemin vers le CSV de l’environnement
        edu_csv,         # Chemin vers le CSV de l’éducation
        demo_csv,        # Chemin vers le CSV de la démographie
        tech_csv,        # Chemin vers le CSV de la technologie
        output_filename="consolidated_data.csv"
    ):
        from pyspark.sql.functions import col, lit, concat, lpad, coalesce, first, trim, sum as spark_sum
        from pyspark.sql.types import StringType

        logger.info("🚀 Génération du fichier consolidé à partir des CSV...")

        # Liste des départements désirés (métropole, sans Corse)
        desired_depts = [
            f"{i:02d}" for i in range(1, 96) if i not in [20]  # exclut le 20 (qui correspond aux codes corse : 2A/2B)
        ]

        # Fichier mapping départements-régions (on suppose que les codes sont déjà formatés, ex. "01", "02", etc.)
        df_depts = self.spark.read.option("header", "true").csv("data/politique/departements-france.csv") \
            .select(
                trim(col("code_region")).alias("region"),
                col("code_departement").alias("dept")
            ).filter(col("dept").isin(desired_depts))

        # Fichiers départementaux
        df_pol = self.spark.read.option("header", "true").option("delimiter", ";").csv(election_csv) \
            .select(
                col("annee").cast("int"),
                lpad(col("code_dept"), 2, "0").alias("dept"),
                col("id_parti").alias("politique (parti)")
            )

        df_sec = self.spark.read.option("header", "true").option("delimiter", ";").csv(security_csv) \
            .select(
                col("Année").cast("int").alias("annee"),
                lpad(col("Département"), 2, "0").alias("dept"),
                col("Délits_total").alias("securite (Nombre_de_délits)")
            )

        df_sat = self.spark.read.option("header", "true").option("delimiter", ";").csv(sante_csv) \
            .select(
                col("Année").cast("int").alias("annee"),
                lpad(col("CODE_DEP"), 2, "0").alias("dept"),
                col("Espérance_Vie").alias("sante (Espérance_de_Vie_H/F)")
            )

        # Pour l'éducation, on convertit le code en entier puis on le formate en 2 chiffres,
        # on ne garde qu'un enregistrement par (annee, dept)
        target_years = [2002, 2007, 2012, 2017, 2022]
        df_ed = self.spark.read.option("header", "true").option("delimiter", ";").csv(edu_csv) \
            .select(
                col("annee_fermeture").cast("int").alias("annee"),
                lpad(col("code_departement").cast("int").cast("string"), 2, "0").alias("dept"),
                col("nombre_total_etablissements").cast("int").alias("education (Nombre_Total_Établissements)")
            ) \
            .filter(col("annee").isin(target_years)) \
            .dropDuplicates(["annee", "dept"])
        # Si plusieurs lignes existent pour un même (annee, dept), on peut agréger avec first(...)

        df_dem = self.spark.read.option("header", "true").option("delimiter", ";").csv(demo_csv) \
            .select(
                col("Année").cast("int").alias("annee"),
                lpad(col("Code_Département"), 2, "0").alias("dept"),
                col("E_Total").alias("demographie (Population_Totale)")
            )

        df_tech = self.spark.read.option("header", "true").option("delimiter", ";").csv(tech_csv) \
            .select(
                col("annee").cast("int"),
                col("dird_pib_france_pourcentages").alias("technologie (Dépenses_en_R&D_en_pourcentages)")
            )

        # Fichiers régionaux (socio-économie et environnement)
        df_soc = self.spark.read.option("header", "true").option("delimiter", ";").csv(socio_csv) \
            .select(
                coalesce(col("Année"), col("année")).cast("int").alias("annee"),
                trim(col("Code_INSEE_Région")).alias("region"),
                col("PIB_par_inflation").alias("socio_economie (PIB_par_Inflation)")
            ) \
            .groupBy("annee", "region") \
            .agg(first("socio_economie (PIB_par_Inflation)").alias("socio_economie (PIB_par_Inflation)")) \
            .join(df_depts, on="region", how="inner") \
            .drop("region")

        df_envr = self.spark.read.option("header", "true").option("delimiter", ";").csv(env_csv) \
            .select(
                coalesce(col("Année"), col("année")).cast("int").alias("annee"),
                trim(col("Code_INSEE_Région")).alias("region"),
                col("Parc_installé_éolien_MW").alias("environnemental (Parc_installé_éolien_MW)")
            ) \
            .groupBy("annee", "region") \
            .agg(first("environnemental (Parc_installé_éolien_MW)").alias("environnemental (Parc_installé_éolien_MW)")) \
            .join(df_depts, on="region", how="inner") \
            .drop("region")

        # Jointure complète
        df_join = df_pol \
            .join(df_sec, ["annee", "dept"], "full_outer") \
            .join(df_soc, ["annee", "dept"], "full_outer") \
            .join(df_sat, ["annee", "dept"], "full_outer") \
            .join(df_envr, ["annee", "dept"], "full_outer") \
            .join(df_ed, ["annee", "dept"], "left") \
            .join(df_dem, ["annee", "dept"], "full_outer") \
            .join(df_tech, ["annee"], "left")

        # Filtrer uniquement sur les années présidentielles et les départements désirés
        df_join = df_join.filter(col("annee").isin(target_years) & col("dept").isin(desired_depts))

        # Clé finale : concaténation de l'année et du code département
        df_join = df_join.withColumn("annee_code_dpt", concat(col("annee").cast("string"), lit("_"), col("dept")))

        # Sélection finale avec les nouveaux noms de colonnes
        df_final = df_join.select(
            "annee_code_dpt",
            "politique (parti)",
            "securite (Nombre_de_délits)",
            "socio_economie (PIB_par_Inflation)",
            "sante (Espérance_de_Vie_H/F)",
            "environnemental (Parc_installé_éolien_MW)",
            "education (Nombre_Total_Établissements)",
            "demographie (Population_Totale)",
            "technologie (Dépenses_en_R&D_en_pourcentages)"
        ).orderBy("annee_code_dpt")

        logger.info("✅ Données consolidées prêtes. Aperçu :")
        df_final.show(10, truncate=False)

        self.save_to_csv(df_final, output_filename)