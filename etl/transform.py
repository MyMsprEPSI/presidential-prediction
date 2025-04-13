# transform.py

import logging
import pandas as pd
from pyspark.sql.functions import (
    col,
    when,
    lit,
    sum as spark_sum,
    round,
    regexp_replace,
    regexp_extract,
    expr,
    trim,
    upper,
    create_map,
)
from pyspark.sql.types import IntegerType, DoubleType, DateType, StringType, StructType, StructField
from pyspark.sql.window import Window
from pyspark.sql import functions as F, types as T, SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from itertools import chain

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Classe permettant de transformer les données extraites avant leur chargement.
    """

    def __init__(self):
        logger.info("🚀 Initialisation du DataTransformer")

    def transform_environmental_data(self, df_env):
        """
        Transforme les données environnementales :
        - Sélectionne uniquement les colonnes nécessaires
        - Remplace les valeurs vides par 0.0 pour les valeurs manquantes d'éolien et solaire
        - Ajoute les données de l'année 2000 pour chaque région avec des valeurs à 0.0
        - Regroupe les données par Code_INSEE_région et Année (somme en cas de doublons)
        - Trie les résultats par région et année
        """

        if df_env is None:
            logger.error("❌ Le DataFrame environnemental est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données environnementales en cours...")

        # Sélection des colonnes nécessaires et cast des valeurs
        df_transformed = df_env.select(
            col("Année").cast("int"),
            col("Code_INSEE_Région"),
            col("Parc_installé_éolien_MW").cast("double"),
            col("Parc_installé_solaire_MW").cast("double"),
        )

        # Remplacement des valeurs nulles par 0.0
        df_transformed = df_transformed.fillna(
            {"Parc_installé_éolien_MW": 0.0, "Parc_installé_solaire_MW": 0.0}
        )

        # Regroupement par région et année pour sommer les valeurs en cas de doublons
        df_grouped = df_transformed.groupBy("Code_INSEE_Région", "Année").agg(
            spark_sum("Parc_installé_éolien_MW").alias("Parc_installé_éolien_MW"),
            spark_sum("Parc_installé_solaire_MW").alias("Parc_installé_solaire_MW"),
        )

        # Récupération des régions uniques présentes dans les données
        regions = df_grouped.select("Code_INSEE_Région").distinct()

        # Création d'un DataFrame contenant l'année 2000 pour chaque région avec valeurs à 0.0
        df_year_2000 = (
            regions.withColumn("Année", lit(2000))
            .withColumn("Parc_installé_éolien_MW", lit(0.0))
            .withColumn("Parc_installé_solaire_MW", lit(0.0))
        )

        # Ajout des données de l'année 2000 au DataFrame principal
        df_final = df_grouped.union(df_year_2000)

        # Tri des données par région et année
        df_final = df_final.orderBy("Code_INSEE_Région", "Année")

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Transformation terminée ! Aperçu des données transformées :",
            df_final,
            15,
        )

    def transform_pib_outre_mer(self, df_pib, region_codes):
        """
        Transforme les données PIB outre-mer :
        - Suppression des lignes inutiles
        - Ajout du code région INSEE à partir du nom du fichier
        - Tri par Région puis Année
        """

        if df_pib is None:
            logger.error("❌ Le DataFrame PIB est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données PIB outre-mer en cours...")

        # Nettoyage des données
        df_cleaned = df_pib.filter(
            (~col("Année").isin(["idBank", "Dernière mise à jour", "Période"]))
            & (col("Année").rlike("^[0-9]{4}$"))
        ).select(
            col("Année").cast("int"),
            col("PIB_en_euros_par_habitant").cast("int"),
            col("source_file"),
        )

        # Ajout du code région INSEE depuis le dictionnaire region_codes
        condition = None
        for file_path, code_region in region_codes.items():
            if condition is None:
                condition = when(col("source_file") == file_path, lit(code_region))
            else:
                condition = condition.when(
                    col("source_file") == file_path, lit(code_region)
                )

        df_final = df_cleaned.withColumn("Code_INSEE_Région", lit(None))
        for file_path, code_region in region_codes.items():
            df_final = df_final.withColumn(
                "Code_INSEE_Région",
                when(col("source_file") == file_path, lit(code_region)).otherwise(
                    col("Code_INSEE_Région")
                ),
            )

        df_final = df_final.drop("source_file")

        # Tri final
        df_final = df_final.orderBy(["Code_INSEE_Région", "Année"])

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Transformation PIB terminée ! Aperçu des données transformées :",
            df_final,
            10,
        )

    def fill_missing_pib_mayotte(self, df_pib):
        """
        Remplit les valeurs manquantes du PIB de Mayotte par régression linéaire.
        """

        logger.info("🚀 Remplissage des valeurs manquantes PIB Mayotte en cours...")

        df_mayotte = df_pib.filter(col("Code_INSEE_Région") == "06")

        known_data = df_mayotte.filter(col("PIB_en_euros_par_habitant").isNotNull())
        unknown_data = df_mayotte.filter(col("PIB_en_euros_par_habitant").isNull())

        assembler = VectorAssembler(inputCols=["Année"], outputCol="features")
        train_data = assembler.transform(known_data).select(
            "features", "PIB_en_euros_par_habitant"
        )

        # Modèle de régression linéaire
        lr = LinearRegression(
            featuresCol="features", labelCol="PIB_en_euros_par_habitant"
        )
        model = lr.fit(train_data)

        # Prédictions sur les données manquantes
        pred_df = assembler.transform(unknown_data)
        pred_result = model.transform(pred_df).select(
            "Année",
            col("prediction").cast("int").alias("PIB_en_euros_par_habitant"),
            "Code_INSEE_Région",
        )

        # Combine les données connues et prédites
        df_mayotte_completed = known_data.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        ).union(pred_result)

        # Autres régions sans modifications
        df_other_regions = df_pib.filter(col("Code_INSEE_Région") != "06")

        # Union finale
        df_final = df_other_regions.union(df_mayotte_completed).orderBy(
            ["Code_INSEE_Région", "Année"]
        )

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Remplissage PIB Mayotte terminé :", df_final, 10
        )

    def combine_all_pib_data(self, df_pib_outremer, df_pib_xlsx, df_pib_2022):
        """
        Combine les données PIB des différentes sources en un seul DataFrame.
        """

        logger.info("🚀 Fusion des données PIB (Outre-mer, Excel, 2022)...")

        # Harmoniser les colonnes
        df_pib_xlsx = df_pib_xlsx.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        )
        df_pib_2022 = df_pib_2022.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        )
        df_pib_outremer = df_pib_outremer.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        )

        # Liste des régions présentes en 2022
        regions_2022 = [
            row["Code_INSEE_Région"]
            for row in df_pib_2022.select("Code_INSEE_Région").distinct().collect()
        ]

        # Identifier les régions absentes en 2022
        missing_regions = (
            df_pib_xlsx.select("Code_INSEE_Région")
            .distinct()
            .filter(~col("Code_INSEE_Région").isin(regions_2022))
        )

        # Ajouter des lignes vides pour les régions absentes en 2022
        if missing_regions.count() > 0:
            df_missing_2022 = missing_regions.withColumn("Année", lit(2022)).withColumn(
                "PIB_en_euros_par_habitant", lit(None).cast("int")
            )
            df_pib_2022 = df_pib_2022.union(df_missing_2022)

        # Fusion des données
        df_final = df_pib_outremer.union(df_pib_xlsx).union(df_pib_2022)

        # **Filtrer les lignes invalides** (Code région doit être numérique et PIB non NULL)
        df_final = df_final.filter(
            (col("Code_INSEE_Région").rlike("^[0-9]+$"))
            & (col("PIB_en_euros_par_habitant").isNotNull())
        )

        # Filtrer et trier
        df_final = df_final.filter((col("Année") >= 2000) & (col("Année") <= 2022))
        df_final = df_final.orderBy(["Code_INSEE_Région", "Année"])

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Fusion des données PIB réussie :", df_final, 10
        )

    def transform_inflation_data(self, df_inflation):
        """
        Transforme les données d'inflation en filtrant les années et en les triant.

        :param df_inflation: DataFrame PySpark contenant les données brutes d'inflation.
        :return: DataFrame PySpark nettoyé et trié.
        """
        if df_inflation is None:
            logger.error("❌ Le DataFrame inflation est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données d'inflation en cours...")

        # Filtrer et trier les données
        df_transformed = df_inflation.orderBy("Année")

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Transformation des données d'inflation réussie :",
            df_transformed,
            10,
        )

    def combine_pib_and_inflation(self, df_pib, df_inflation):
        """
        Combine les données PIB et Inflation, et calcule le ratio PIB_par_inflation avec arrondi à 2 décimales.

        :param df_pib: DataFrame PySpark contenant le PIB par région.
        :param df_inflation: DataFrame PySpark contenant l'inflation nationale.
        :return: DataFrame PySpark combiné avec le calcul du PIB ajusté par l'inflation.
        """
        if df_pib is None or df_inflation is None:
            logger.error("❌ L'un des DataFrames est vide. Impossible de les combiner.")
            return None

        logger.info("🚀 Fusion des données PIB et Inflation...")

        # Joindre PIB et Inflation sur la colonne Année
        df_combined = df_pib.join(df_inflation, "Année", "left")

        # Utiliser le bon nom de colonne pour l'inflation et arrondir à 2 décimales
        df_combined = df_combined.withColumn(
            "Évolution_des_prix_à_la_consommation",
            round(col("Évolution_des_prix_à_la_consommation"), 2),
        )

        df_combined = df_combined.withColumn(
            "PIB_par_inflation",
            round(
                col("PIB_en_euros_par_habitant")
                / (1 + col("Évolution_des_prix_à_la_consommation") / 100),
                2,
            ),
        )

        # Trier les résultats
        df_combined = df_combined.orderBy("Code_INSEE_Région", "Année")

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Fusion des données PIB et Inflation réussie :", df_combined, 10
        )

    def transform_technologie_data(self, df):
        """
        Transforme les données de technologie.

        :param df: DataFrame PySpark brut
        :return: DataFrame PySpark transformé
        """
        if df is None:
            logger.error("❌ Le DataFrame technologie est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données de technologie en cours...")

        try:
            return self._extracted_from_transform_technologie_data_15(df)
        except Exception as e:
            logger.error(f"❌ Erreur lors de la transformation des données : {str(e)}")
            return None

    # TODO Rename this here and in `transform_technologie_data`
    def _extracted_from_transform_technologie_data_15(self, df):
        df_transformed = self._select_and_rename_columns(df)
        df_transformed = self._round_percentages(df_transformed)
        df_transformed = self._clean_years(df_transformed)
        df_transformed = self._replace_nan_year(df_transformed)

        logger.info("✅ Transformation des données de technologie réussie")
        return df_transformed

    def _select_and_rename_columns(self, df):
        """Sélectionne et renomme les colonnes."""
        return df.select(
            col("_c0").alias("annee").cast("string"),
            col("DIRD/PIB  France").alias("dird_pib_france_pourcentages").cast("float"),
        )

    def _round_percentages(self, df):
        """Arrondit les pourcentages à 2 décimales."""
        return df.withColumn(
            "dird_pib_france_pourcentages",
            round(col("dird_pib_france_pourcentages"), 2),
        )

    def _clean_years(self, df):
        """Nettoie les années en supprimant '.0'."""
        return df.withColumn("annee", regexp_replace("annee", "\.0", ""))

    def _replace_nan_year(self, df):
        """Remplace les valeurs 'NaN' dans la colonne 'annee' par '2023'."""
        return df.withColumn(
            "annee", when(col("annee") == "NaN", "2023").otherwise(col("annee"))
        )

    def transform_election_data_1965_2012(self, list_df):
        """
        Transforme et agrège les fichiers CSV 1965-2012.
        - Unpivot via STACK
        - Agrégation par candidat
        - Sélection du candidat gagnant par département et année
        - Nettoyage du nom du candidat
        :param list_df: liste de DataFrames bruts
        :return: DataFrame PySpark final (année, code_dept, candidat, total_voix)
        """
        if not list_df:
            logger.warning("Liste de DataFrames 1965-2012 vide.")
            return None
        results = []
        # Boucle sur chacun des DF (un par fichier CSV)
        for df in list_df:
            # Colonnes clés (à ne pas unpivoter)
            key_columns = {
                "Code département",
                "Code département0",
                "Code département1",
                "département",
                "circonscription",
                "Inscrits",
                "Votants",
                "Exprimés",
                "Blancs et nuls",
                "filename",
                "annee",
            }

            # Détermine la bonne colonne de département
            if "Code département" in df.columns:
                dept_col = "Code département"
            elif "Code département0" in df.columns:
                dept_col = "Code département0"
            else:
                # Pas de colonne attendue
                continue

            # Colonnes candidats
            candidate_columns = [c for c in df.columns if c not in key_columns]
            n = len(candidate_columns)
            if n == 0:
                continue

            # Expression stack pour unpivot
            expr_parts = []
            for c in candidate_columns:
                escaped_col = c.replace("'", "''")
                expr_parts.append(f"'{escaped_col}', cast(`{c}` as int)")
            expr = f"stack({n}, {', '.join(expr_parts)}) as (candidat, voix)"

            # Unpivot
            df_unpivot = df.select(
                "annee", F.col(dept_col).alias("code_dept"), F.expr(expr)
            )

            # Agrégation par département / candidat / année
            df_agg = df_unpivot.groupBy("annee", "code_dept", "candidat").agg(
                F.sum("voix").alias("total_voix")
            )

            # Sélection du gagnant par dept + année
            windowSpec = Window.partitionBy("annee", "code_dept").orderBy(
                F.desc("total_voix")
            )
            df_winner = df_agg.withColumn(
                "rank", F.row_number().over(windowSpec)
            ).filter(F.col("rank") == 1)

            # Nettoyage du nom du candidat
            df_winner = df_winner.withColumn("gagnant", F.col("candidat"))
            df_winner = df_winner.withColumn(
                "gagnant", F.trim(F.regexp_replace(F.col("gagnant"), r"\([^)]*\)", ""))
            )
            df_winner = df_winner.withColumn(
                "gagnant",
                when(F.col("gagnant") == "SARKOZY", "Nicolas SARKOZY")
                .when(F.col("gagnant") == "CHIRAC", "Jacques CHIRAC")
                .when(F.col("gagnant") == "MITTERRAND", "François MITTERRAND")
                .when(F.col("gagnant") == "DE GAULLE", "Charles DE GAULLE")
                .when(F.col("gagnant") == "GISCARD DESTAING", "Valéry GISCARD DESTAING")
                .when(F.col("gagnant") == "POMPIDOU", "Georges POMPIDOU")
                .when(F.col("gagnant") == "POHER", "Alain POHER")
                .when(F.col("gagnant") == "JOSPIN", "Lionel JOSPIN")
                .when(F.col("gagnant") == "ROYAL", "Ségolène ROYAL")
                .when(F.col("gagnant") == "HOLLANDE", "François HOLLANDE")
                .when(F.col("gagnant") == "MACRON", "Emmanuel MACRON")
                .when(F.col("gagnant") == "LE PEN", "Marine LE PEN")
                .otherwise(F.col("gagnant")),
            )

            # Sélection colonnes finales
            results.append(
                df_winner.select(
                    "annee",
                    "code_dept",
                    F.col("gagnant").alias("candidat"),
                    "total_voix",
                )
            )

        # Union de tous les résultats
        if results:
            final_df = results[0]
            for df_r in results[1:]:
                final_df = final_df.union(df_r)

            # Normalisation du code_dept (ex: passer '1' -> '01')
            final_df = (
                final_df.withColumn(
                    "code_dept_norm",
                    F.when(
                        F.col("code_dept").rlike("^[0-9]$"),
                        F.lpad(F.col("code_dept"), 2, "0"),
                    ).otherwise(F.col("code_dept")),
                )
                .drop("code_dept")
                .withColumnRenamed("code_dept_norm", "code_dept")
            )

            return final_df

        else:
            logger.warning("Aucune donnée agrégée pour 1965-2012.")
            return None

    def transform_election_data_2017(self, df_2017_raw):
        """
        Transforme le fichier Excel 2017 :
        - Sélection du candidat gagnant par département
        - Nettoyage (codes spéciaux pour régions d outre-mer)
        """
        if df_2017_raw is None:
            logger.warning("DataFrame 2017 vide.")
            return None

        df_2017 = (
            df_2017_raw.withColumnRenamed("Code du département", "code_dept")
            .withColumn(
                "candidat1", F.concat(F.col("Nom17"), F.lit(" "), F.col("Prénom18"))
            )
            .withColumn(
                "candidat2", F.concat(F.col("Nom23"), F.lit(" "), F.col("Prénom24"))
            )
            .select(
                F.col("code_dept").cast("string"),
                F.col("Libellé du département"),
                F.col("Voix19").alias("voix1").cast("int"),
                F.col("Voix25").alias("voix2").cast("int"),
                "candidat1",
                "candidat2",
            )
        )

        # On crée un DataFrame par candidat
        df_2017_candidate1 = df_2017.select(
            "code_dept",
            F.col("candidat1").alias("candidat"),
            F.col("voix1").alias("voix"),
            F.col("Libellé du département"),
        )

        df_2017_candidate2 = df_2017.select(
            "code_dept",
            F.col("candidat2").alias("candidat"),
            F.col("voix2").alias("voix"),
            F.col("Libellé du département"),
        )

        # Union des deux candidats
        df_2017_norm = df_2017_candidate1.union(df_2017_candidate2).withColumn(
            "annee", F.lit("2017")
        )

        # 1. Appliquer le mapping pour les codes spéciaux et la Corse
        df_2017_norm = df_2017_norm.withColumn(
            "code_dept_norm",
            F.when(F.col("Libellé du département") == "Guadeloupe", "ZA")
            .when(F.col("Libellé du département") == "Martinique", "ZB")
            .when(F.col("Libellé du département") == "Guyane", "ZC")
            .when(F.col("Libellé du département") == "La Réunion", "ZD")
            .when(F.col("Libellé du département") == "Mayotte", "ZM")
            .when(F.col("Libellé du département") == "Nouvelle-Calédonie", "ZN")
            .when(F.col("Libellé du département") == "Polynésie française", "ZP")
            .when(F.col("Libellé du département") == "Saint-Pierre-et-Miquelon", "ZS")
            .when(
                F.col("Libellé du département") == "Saint-Martin/Saint-Barthélemy", "ZX"
            )
            .when(F.col("Libellé du département") == "Wallis et Futuna", "ZW")
            .when(
                F.col("Libellé du département") == "Français établis hors de France",
                "ZZ",
            )
            .when(F.col("Libellé du département") == "Corse-du-Sud", "2A")
            .when(F.col("Libellé du département") == "Haute-Corse", "2B")
            .otherwise(F.col("code_dept")),
        )

        # 1. Supprimer la terminaison ".0" dans la colonne "code_dept_norm"
        df_final_2017 = df_2017_norm.withColumn(
            "code_dept_final", F.regexp_replace(F.col("code_dept_norm"), r"\.0$", "")
        )

        # 2. (Optionnel) Si vous souhaitez que les codes sur un seul chiffre soient affichés sur 2 chiffres (ex. "1" -> "01")
        df_final_2017 = df_final_2017.withColumn(
            "code_dept_final",
            F.when(
                F.col("code_dept_final").rlike("^[0-9]$"),
                F.lpad(F.col("code_dept_final"), 2, "0"),
            ).otherwise(F.col("code_dept_final")),
        )

        # 3. Supprimer les colonnes intermédiaires et renommer la colonne finale en "code_dept"
        df_final_2017 = df_final_2017.drop(
            "code_dept", "code_dept_norm", "Libellé du département"
        ).withColumnRenamed("code_dept_final", "code_dept")

        # Pour chaque département, on garde le candidat avec le maximum de voix
        w_dept = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
        return (
            df_final_2017.withColumn("rank", F.row_number().over(w_dept))
            .filter(F.col("rank") == 1)
            .select("annee", "code_dept", "candidat", "voix")
        )

    def transform_election_data_2022(self, df_2022_raw):
        """
        Transforme le fichier Excel 2022 :
        - Sélection du gagnant par département
        - Normalisation du nom du candidat (Emmanuel MACRON, Marine LE PEN, etc.)
        """
        if df_2022_raw is None:
            logger.warning("DataFrame 2022 vide.")
            return None

        # Pour 2022, on suppose que chaque ligne correspond déjà à un candidat,
        # avec "Code du département", "Nom", "Prénom" et "Voix".
        df_2022 = (
            df_2022_raw.withColumnRenamed("Code du département", "code_dept")
            .withColumn("candidat", F.concat(F.col("Nom"), F.lit(" "), F.col("Prénom")))
            .select(
                F.col("code_dept").cast("string"),
                "candidat",
                F.col("Voix").alias("voix"),
            )
            .withColumn("annee", F.lit("2022"))
        )

        # On agrège par département pour sélectionner le candidat gagnant (le plus de voix)
        w_dept_2022 = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
        return (
            df_2022.withColumn("rank", F.row_number().over(w_dept_2022))
            .filter(F.col("rank") == 1)
            .select("annee", "code_dept", "candidat", "voix")
        )

    def combine_all_years(self, df_1965_2012, df_2017, df_2022):
        """
        Combine les DataFrames de 1965-2012, 2017 et 2022.
        Applique les mappings DOM-TOM (ZA->971 etc.) et renomme la colonne 'voix' en 'total_voix'.
        """
        if df_1965_2012 is None and df_2017 is None and df_2022 is None:
            logger.warning("Aucun DataFrame à combiner.")
            return None

        # Union 2017 et 2022
        df_final = df_2017.union(df_2022)

        # 1. Appliquer le mapping pour les codes spéciaux
        df_final = df_final.withColumn(
            "code_dept",
            F.when(F.col("code_dept") == "ZA", "971")
            .when(F.col("code_dept") == "ZB", "972")
            .when(F.col("code_dept") == "ZC", "973")
            .when(F.col("code_dept") == "ZD", "974")
            .when(F.col("code_dept") == "ZM", "976")
            .when(F.col("code_dept") == "ZN", "988")
            .when(F.col("code_dept") == "ZP", "987")
            .when(F.col("code_dept") == "ZS", "975")
            .when(F.col("code_dept") == "ZX", "971")
            .when(F.col("code_dept") == "ZW", "986")
            .when(F.col("code_dept") == "ZZ", "99")
            .otherwise(F.col("code_dept")),
        )

        # 1.1 Normalisation des candidats avec prénom et nom
        df_final = df_final.withColumn(
            "candidat",
            F.when(F.col("candidat") == "MACRON Emmanuel", "Emmanuel MACRON").when(
                F.col("candidat") == "LE PEN Marine", "Marine LE PEN"
            ),
        )

        # 2. Appliquer le format int pour les voix
        df_final = df_final.withColumn("voix", F.col("voix").cast("int"))

        # 3. Renommer la colonne "voix" en "total_voix"
        df_final = df_final.withColumnRenamed("voix", "total_voix")

        # 4. Sélection des colonnes d'intérêt
        df_1965_2012 = df_1965_2012.select(
            "annee", "code_dept", "candidat", "total_voix"
        )

        # 5. Union des deux DataFrames
        df_final_csv = df_final.union(df_1965_2012)

        # Tri final
        df_final_csv = df_final_csv.orderBy("annee", "code_dept")

        return df_final_csv

    def transform_life_expectancy_data(self, df_life, df_departments):
        """
        Transforme les données d'espérance de vie à la naissance pour hommes et femmes pour
        obtenir une colonne unique "Espérance_Vie" correspondant à la moyenne des valeurs hommes et femmes.

        Étapes :
        - Filtrage des lignes dont le libellé commence par "Espérance de vie à la naissance - Hommes" ou "Espérance de vie à la naissance - Femmes"
        - Extraction du genre et du nom de département ou région depuis le libellé
        - Sélection des colonnes des années (2000 à 2022) et conversion du format large en format long via STACK
        - Pivot sur la colonne "Genre" pour obtenir deux colonnes ("Hommes" et "Femmes")
        - Normalisation des noms pour ne conserver que les départements réels (à l'aide du DataFrame des départements)
        - Jointure avec le DataFrame des départements pour récupérer le code département réel (CODE_DEP)
        - Calcul de la moyenne de "Hommes" et "Femmes" et création d'une colonne unique "Espérance_Vie"

        :param df_life: DataFrame PySpark contenant les données brutes d'espérance de vie.
        :param df_departments: DataFrame PySpark contenant les données des départements.
        :return: DataFrame final avec colonnes CODE_DEP, Année, Espérance_Vie.
        """
        if df_life is None:
            logger.error("❌ Le DataFrame d'espérance de vie est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données d'espérance de vie en cours...")

        # Filtrer les lignes d'intérêt
        df_filtered = df_life.filter(
            (col("Libellé").rlike("^Espérance de vie à la naissance - Hommes"))
            | (col("Libellé").rlike("^Espérance de vie à la naissance - Femmes"))
        )

        # Extraire le genre et le "nom de département ou région" depuis le libellé
        df_filtered = df_filtered.withColumn(
            "Genre",
            regexp_extract(
                col("Libellé"),
                r"Espérance de vie à la naissance - (Hommes|Femmes) - (.*)",
                1,
            ),
        ).withColumn(
            "Département",
            trim(
                regexp_extract(
                    col("Libellé"),
                    r"Espérance de vie à la naissance - (Hommes|Femmes) - (.*)",
                    2,
                )
            ),
        )

        # Sélectionner les colonnes des années de 2000 à 2022
        years = [str(year) for year in range(2000, 2023)]
        selected_cols = ["Libellé", "Genre", "Département"] + years
        df_selected = df_filtered.select(*selected_cols)

        # Conversion du format large en format long via STACK
        n_years = len(years)
        stack_expr = "stack({0}, {1}) as (Annee, Esperance_de_vie)".format(
            n_years, ", ".join([f"'{year}', `{year}`" for year in years])
        )
        df_long = df_selected.select("Genre", "Département", expr(stack_expr))
        df_long = df_long.withColumn(
            "Annee", col("Annee").cast(IntegerType())
        ).withColumn("Esperance_de_vie", col("Esperance_de_vie").cast(DoubleType()))
        df_long = df_long.filter(col("Annee").between(2000, 2022))

        # Pivot pour créer des colonnes pour Hommes et Femmes
        df_pivot = (
            df_long.groupBy("Département", "Annee")
            .pivot("Genre", ["Hommes", "Femmes"])
            .agg(F.first("Esperance_de_vie"))
        )

        # Fonction de normalisation des noms
        def normalize_dept(column):
            norm = F.lower(trim(column))
            # Remplacer les accents par leurs équivalents non accentués
            norm = F.translate(norm, "éèêëàâäîïôöùûüç", "eeeeaaaiioouuuc")
            # Supprimer tirets, apostrophes et espaces
            norm = F.regexp_replace(norm, "[-' ]", "")
            return norm

        # Appliquer la normalisation sur le DataFrame pivoté
        df_pivot = df_pivot.withColumn(
            "Département_norm", normalize_dept(col("Département"))
        )
        # Normaliser le DataFrame des départements
        df_depts_norm = df_departments.withColumn(
            "nom_departement_norm", normalize_dept(col("nom_departement"))
        )

        # Filtrage : ne conserver que les lignes correspondant à des départements réels
        valid_dept_names = [
            row["nom_departement_norm"]
            for row in df_depts_norm.select("nom_departement_norm").distinct().collect()
        ]
        logger.info(
            "Liste des départements valides (normalisés) : "
            + ", ".join(valid_dept_names)
        )
        df_pivot = df_pivot.filter(col("Département_norm").isin(valid_dept_names))

        # Jointure pour associer le code de département réel
        df_joined = df_pivot.join(
            df_depts_norm,
            df_pivot["Département_norm"] == df_depts_norm["nom_departement_norm"],
            "left",
        )

        # Calcul de la moyenne des deux colonnes pour obtenir une seule colonne "Espérance_Vie"
        df_final = df_joined.select(
            df_depts_norm["code_departement"].alias("CODE_DEP"),
            col("Annee").alias("Année"),
            round(((col("Hommes") + col("Femmes")) / 2), 2).alias("Espérance_Vie"),
        ).orderBy("CODE_DEP", "Année")

        logger.info("✅ Transformation terminée ! Aperçu :")
        df_final.show(10, truncate=False)

        # Affichage de débogage : lignes non associées (si besoin)
        df_unmatched = df_joined.filter(df_depts_norm["code_departement"].isNull())
        logger.info("Lignes non associées après jointure :")
        df_unmatched.select("Département", "Département_norm").distinct().show(
            truncate=False
        )

        return df_final

    def fill_missing_mayotte_life_expectancy(self, df_final):
        """
        Complète les valeurs manquantes pour Mayotte (CODE_DEP = "976")
        dans le DataFrame final en utilisant une régression linéaire sur l'année.
        On entraîne un modèle sur les données connues (colonne 'Espérance_Vie')
        puis on prédit pour les années manquantes.

        :param df_final: DataFrame final avec colonnes CODE_DEP, Année, Espérance_Vie
        :return: DataFrame final avec les valeurs manquantes pour Mayotte complétées
                et arrondies à 2 décimales.
        """
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import LinearRegression
        from pyspark.sql.functions import col, when, round

        # Filtrer uniquement les données de Mayotte
        df_mayotte = df_final.filter(col("CODE_DEP") == "976")

        # Séparer les données connues et inconnues pour la colonne "Espérance_Vie"
        known = df_mayotte.filter(col("Espérance_Vie").isNotNull())
        unknown = df_mayotte.filter(col("Espérance_Vie").isNull())

        # Préparer les données pour la régression
        assembler = VectorAssembler(inputCols=["Année"], outputCol="features")
        train = assembler.transform(known).select("Année", "features", "Espérance_Vie")

        # Entraîner le modèle de régression linéaire
        lr = LinearRegression(featuresCol="features", labelCol="Espérance_Vie")
        model = lr.fit(train)

        # Prédire pour les années manquantes
        pred = assembler.transform(unknown)
        pred = model.transform(pred).select(
            "Année", col("prediction").alias("pred_value")
        )

        # Remplacer les valeurs manquantes par la prédiction (arrondie à 2 décimales)
        df_mayotte_filled = (
            df_mayotte.alias("base")
            .join(pred.alias("pred"), on="Année", how="left")
            .withColumn(
                "Espérance_Vie_new",
                when(
                    col("base.Espérance_Vie").isNull(), round(col("pred.pred_value"), 2)
                ).otherwise(round(col("base.Espérance_Vie"), 2)),
            )
            .select(
                col("base.CODE_DEP").alias("CODE_DEP"),
                col("base.Année").alias("Année"),
                col("Espérance_Vie_new").alias("Espérance_Vie"),
            )
        )

        # Conserver les données des autres départements
        df_other = df_final.filter(col("CODE_DEP") != "976")

        # Fusionner et trier le DataFrame final
        return df_other.unionByName(df_mayotte_filled).orderBy("CODE_DEP", "Année")

    def transform_education_data(self, df):
        """
        Transforme et nettoie les données d'éducation issues du CSV 'fr-en-etablissements-fermes.csv'.
        Étapes de transformation :
          1. Suppression des doublons.
          2. Standardisation de toutes les colonnes textuelles (conversion en minuscules, suppression des espaces,
             remplacement des valeurs nulles par "non spécifié").
          3. Conversion de la colonne "date_fermeture" en type Date et extraction de l'année dans "annee_fermeture".
          4. Normalisation du code postal : remplacement des valeurs nulles par "00000", puis suppression des espaces.
          5. Séparation des secteurs public et privé à partir de la colonne "secteur_public_prive_libe".
        :param df: DataFrame Spark brut issu du fichier CSV d'éducation.
        :return: DataFrame nettoyé et transformé.
        """

        logger.info("🚀 Transformation des données d'éducation en cours...")

        # 1. Suppression des doublons
        df = df.dropDuplicates()

        # 2. Standardisation des colonnes textuelles
        for column in df.columns:
            df = df.withColumn(
                column,
                F.when(
                    F.col(column).isNotNull(), F.trim(F.lower(F.col(column)))
                ).otherwise(F.lit("non spécifié")),
            )

        # 3. Conversion de 'date_fermeture' en DateType et extraction de l'année
        if "date_fermeture" in df.columns:
            df = df.withColumn(
                "date_fermeture", F.col("date_fermeture").cast(DateType())
            )
            df = df.withColumn("annee_fermeture", F.year(F.col("date_fermeture")))

        # 4. Normalisation du code postal
        if "code_postal" in df.columns:
            df = df.withColumn(
                "code_postal",
                F.when(F.col("code_postal").isNull(), F.lit("00000")).otherwise(
                    F.trim(F.col("code_postal"))
                ),
            )

        # 5. Séparation du secteur public/privé
        if "secteur_public_prive_libe" in df.columns:
            df = df.withColumn(
                "secteur_public",
                F.when(F.col("secteur_public_prive_libe") == "public", 1).otherwise(0),
            )
            df = df.withColumn(
                "secteur_prive",
                F.when(F.col("secteur_public_prive_libe") == "privé", 1).otherwise(0),
            )

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Transformation des données d'éducation réussie.", df, 5
        )

    def calculate_closed_by_year_and_dept_education(self, df):
        """
        Calcule le nombre d'établissements fermés par année et par département à partir des données d'éducation.
        Regroupe par 'annee_fermeture', 'code_departement' et 'libelle_departement', puis agrège :
        - Le nombre total d'établissements (count sur "numero_uai"),
        - Le nombre d'établissements fermés dans le secteur public (sum de "secteur_public"),
        - Le nombre dans le secteur privé (sum de "secteur_prive"),
        - Les pourcentages correspondants (arrondis à 2 décimales).

        Ensuite, pour chaque département présent, les combinaisons manquantes pour les années cibles
        (2002, 2007, 2012, 2017, 2022) sont complétées avec des valeurs par défaut (0).

        :param df: DataFrame nettoyé d'éducation, incluant les colonnes "annee_fermeture",
                "code_departement", "libelle_departement", "numero_uai", "secteur_public" et "secteur_prive".
        :return: DataFrame avec les statistiques par année et département complétées.
        """
        import pyspark.sql.functions as F
        from pyspark.sql.functions import col, lit, when

        logger.info(
            "🚀 Calcul des statistiques de fermetures d'établissements par département et année..."
        )

        # Agrégation initiale - éviter le regroupement par libelle_departement pour réduire la mémoire
        df_grouped = df.groupBy("annee_fermeture", "code_departement").agg(
            F.first("libelle_departement").alias("libelle_departement"),
            F.count("numero_uai").alias("nombre_total_etablissements"),
            F.sum("secteur_public").alias("nb_public"),
            F.sum("secteur_prive").alias("nb_prive"),
        )

        # Calculer les pourcentages avec une expression sécurisée pour éviter division par zéro
        df_grouped = df_grouped.withColumn(
            "pct_public",
            F.round(
                F.when(
                    F.col("nombre_total_etablissements") > 0,
                    F.col("nb_public") * 100.0 / F.col("nombre_total_etablissements"),
                ).otherwise(0.0),
                2,
            ),
        ).withColumn(
            "pct_prive",
            F.round(
                F.when(
                    F.col("nombre_total_etablissements") > 0,
                    F.col("nb_prive") * 100.0 / F.col("nombre_total_etablissements"),
                ).otherwise(0.0),
                2,
            ),
        )

        # Liste des années cibles pour lesquelles on souhaite forcer une présence
        target_years = [2002, 2007, 2012, 2017, 2022]

        # Récupérer uniquement les départements uniques pour éviter de multiplier les données
        df_depts = (
            df.select("code_departement", "libelle_departement").distinct().cache()
        )

        # Créer des données pour les années manquantes pour chaque département
        result_dfs = []

        for year in target_years:
            # Pour chaque année cible, créer un DataFrame avec cette année et tous les départements
            df_year = df_depts.withColumn("annee_fermeture", lit(year))

            # Jointure gauche avec les données existantes
            df_year_completed = df_year.join(
                df_grouped.filter(F.col("annee_fermeture") == year),
                on=["code_departement", "annee_fermeture", "libelle_departement"],
                how="left",
            )

            # Remplir les valeurs manquantes
            df_year_completed = df_year_completed.na.fill(
                {
                    "nombre_total_etablissements": 0,
                    "nb_public": 0,
                    "nb_prive": 0,
                    "pct_public": 0.0,
                    "pct_prive": 0.0,
                }
            )

            result_dfs.append(df_year_completed)

        # Union de tous les résultats par année avec les données originales filtrées sur les années non-cibles
        df_other_years = df_grouped.filter(~F.col("annee_fermeture").isin(target_years))
        result_dfs.append(df_other_years)

        # Effectuer l'union de tous les DataFrames
        df_completed = result_dfs[0]
        for i in range(1, len(result_dfs)):
            df_completed = df_completed.unionByName(
                result_dfs[i], allowMissingColumns=True
            )

        # Nettoyer les valeurs nulles qui pourraient rester dans les colonnes numériques
        df_completed = df_completed.na.fill(
            {
                "nombre_total_etablissements": 0,
                "nb_public": 0,
                "nb_prive": 0,
                "pct_public": 0.0,
                "pct_prive": 0.0,
            }
        )

        # Tri final par année et département
        df_completed = df_completed.orderBy("annee_fermeture", "code_departement")

        # Libérer la mémoire du cache
        df_depts.unpersist()

        return self._extracted_from__extracted_from_combine_election_and_orientation_politique_52_116(
            "✅ Calcul des statistiques complété. Aperçu :", df_completed, 10
        )

    def transform_security_data(self, df):
        """
        Transforme les données de sécurité exclusivement avec pandas.
        Format de sortie: Année, Département, Délits_total avec séparateur virgule.

        :param df: Non utilisé dans cette version
        :return: DataFrame pandas avec colonnes (Année, Département, Délits_total)
        """
        import pandas as pd
        from collections import defaultdict
        import os

        logger.info(
            "🚀 Transformation des données de sécurité avec pandas uniquement..."
        )

        try:
            # Chemin du fichier Excel
            fichier_excel = "data/securite/tableaux-4001-ts.xlsx"

            if not os.path.exists(fichier_excel):
                logger.error(f"❌ Fichier Excel non trouvé: {fichier_excel}")
                return None

            logger.info(f"📂 Chargement du fichier Excel: {fichier_excel}")
            xls = pd.ExcelFile(fichier_excel)

            # Liste des feuilles correspondant aux départements
            departements = [sheet for sheet in xls.sheet_names if sheet.isdigit()]
            logger.info(f"✓ {len(departements)} départements identifiés")

            # Dictionnaire pour stocker les résultats {(département, année): total}
            resultats = defaultdict(int)

            # Plage d'années ciblée
            annees_cibles = set(range(1996, 2023))  # de 1996 à 2022 inclus

            # Traitement de chaque feuille
            for dept in departements:
                try:
                    resultats = self._process_department_sheet(dept, xls, annees_cibles, resultats)
                except Exception as e:
                    logger.error(f"❌ Erreur sur le département {dept}: {str(e)}")
                    
            # Création du DataFrame final
            df_final = self._create_final_security_dataframe(resultats)

            logger.info(
                "✅ Transformation des données de sécurité terminée avec succès"
            )
            return df_final

        except Exception as e:
            logger.error(f"❌ Erreur lors de la transformation: {str(e)}")
            import traceback

            logger.error(traceback.format_exc())
            return None
            
    def _process_department_sheet(self, dept, xls, annees_cibles, resultats):
        """
        Traite une feuille de département et met à jour le dictionnaire des résultats.
        
        :param dept: Code du département
        :param xls: Objet ExcelFile pandas
        :param annees_cibles: Ensemble des années à considérer
        :param resultats: Dictionnaire des résultats à mettre à jour
        :return: Dictionnaire des résultats mis à jour
        """
        import pandas as pd
        
        logger.info(f"✨ Traitement du département {dept}...")
        df_dept = xls.parse(dept)
        df_dept = df_dept.dropna(how="all")  # retirer les lignes totalement vides

        for col in df_dept.columns:
            if isinstance(col, str) and col.startswith("_"):
                try:
                    annee = int(col.split("_")[1])
                    if annee in annees_cibles:
                        resultats[(annee, dept)] += df_dept[col].sum(skipna=True)
                except Exception as e:
                    logger.warning(f"⚠️ Problème avec la colonne {col}: {str(e)}")
                    
        return resultats
        
    def _create_final_security_dataframe(self, resultats):
        """
        Crée le DataFrame final à partir du dictionnaire des résultats.
        
        :param resultats: Dictionnaire {(année, département): total}
        :return: DataFrame pandas formaté
        """
        import pandas as pd
        
        logger.info("✓ Création du DataFrame final...")
        df_final = pd.DataFrame(
            [
                {"Année": annee, "Département": dept, "Délits_total": int(total)}
                for (annee, dept), total in resultats.items()
            ]
        )

        # Formater le code département pour avoir toujours 2 chiffres
        df_final["Département"] = df_final["Département"].apply(
            lambda x: x.zfill(2) if x.isdigit() and len(x) == 1 else x
        )

        # Tri pour lisibilité (d'abord par année, puis par département)
        df_final = df_final.sort_values(by=["Année", "Département"])

        # Afficher des statistiques
        if not df_final.empty:
            logger.info(
                f"✓ Période couverte: de {df_final['Année'].min()} à {df_final['Année'].max()}"
            )
            logger.info(
                f"✓ {df_final['Département'].nunique()} départements traités"
            )
            logger.info(f"✓ Total de {len(df_final)} lignes de données générées")

            # Échantillon des données
            logger.info(
                "✓ Échantillon des données (format: Année,Département,Délits_total):"
            )
            for idx, row in df_final.head(5).iterrows():
                logger.info(
                    f"{row['Année']},{row['Département']},{int(row['Délits_total'])}"
                )
                
        return df_final

    def transform_demography_data(self, df):
        """
        Transforme les données démographiques issues du CSV en :
        - Renommant les colonnes principales
        - Nettoyant la colonne du code département et en filtrant les lignes parasites
        - Conserver la colonne 'Année' (provenant du nom de la feuille Excel)
        - Classer par Année, puis par département
        """
        from pyspark.sql.functions import col, trim, regexp_replace, split, when

        if df is None:
            logger.error("❌ Le DataFrame de démographie est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données démographiques en cours...")

        # 1) Renommage des colonnes principales (selon votre CSV)
        #    Assurez-vous que ces noms correspondent à votre structure réelle
        df = (
            df.withColumnRenamed("Départements", "Code_Département")
            .withColumnRenamed("Unnamed: 1", "Nom_Département")
            .withColumnRenamed("Ensemble", "E_Total")
            .withColumnRenamed("Hommes", "H_Total")
            .withColumnRenamed("Femmes", "F_Total")
        )

        # 2) Renommage des colonnes des tranches d'âge
        df = (
            df.withColumnRenamed("Unnamed: 3", "E_0_19_ans")
            .withColumnRenamed("Unnamed: 4", "E_20_39_ans")
            .withColumnRenamed("Unnamed: 5", "E_40_59_ans")
            .withColumnRenamed("Unnamed: 6", "E_60_74_ans")
            .withColumnRenamed("Unnamed: 7", "E_75_et_plus")
        )

        # 3) Filtrer les lignes parasites
        #    (celles qui commencent par "Source", contiennent "France" ou "DOM", etc.)
        #    et aussi la ligne d'en-tête répétée (repérée par "0 à 19 ans" dans E_0_19_ans)
        df = df.filter(
            ~col("Code_Département").startswith("Source")
            & ~col("Code_Département").contains("France")
            & ~col("Code_Département").contains("DOM")
            & ~col("Code_Département").startswith("NB")
            & ~col("Code_Département").startswith("Population")
            & (col("E_0_19_ans") != "0 à 19 ans")
        )

        # 4) Nettoyer la colonne Code_Département : suppression des espaces et guillemets
        df = df.withColumn("Code_Département", trim(col("Code_Département")))
        df = df.withColumn(
            "Code_Département", regexp_replace(col("Code_Département"), '"', "")
        )

        # 5) Extraire le code (premier token) et éventuellement le nom depuis la colonne Code_Département
        df = df.withColumn(
            "first_token", split(col("Code_Département"), " ", 2)[0]
        ).withColumn("remainder", split(col("Code_Département"), " ", 2)[1])

        # 6) Ne conserver que les lignes dont le premier token correspond à un code département valide
        df = df.filter(col("first_token").rlike("^(2A|2B|[0-9]{1,3})$"))

        # 7) Remplacer Code_Département par le premier token
        df = df.withColumn("Code_Département", col("first_token"))

        # 8) Si Nom_Département est vide, utiliser remainder comme nom
        df = df.withColumn(
            "Nom_Département",
            when(
                (col("Nom_Département").isNull()) | (col("Nom_Département") == ""),
                col("remainder"),
            ).otherwise(col("Nom_Département")),
        )

        # 9) Supprimer les colonnes temporaires
        df = df.drop("first_token", "remainder")

        # 10) Sélectionner et réorganiser les colonnes dans l'ordre souhaité
        #     On inclut désormais "Année" pour la conserver et trier ensuite.
        final_columns = [
            "Année",
            "Code_Département",
            "Nom_Département",
            "E_Total",
            "H_Total",
            "F_Total",
            "E_0_19_ans",
            "E_20_39_ans",
            "E_40_59_ans",
            "E_60_74_ans",
            "E_75_et_plus",
        ]

        # Vérifier que la colonne "Année" existe bien dans votre DataFrame
        # (au cas où la ligne "df['Année'] = sheet" a bien été créée à l'extraction)
        available_cols = [c for c in final_columns if c in df.columns]
        df_final = df.select(*available_cols)

        # 11) Classer par Année, puis par Code_Département
        #     Si Année est stockée en string, on peut la convertir en int si c'est un simple nombre
        df_final = (
            df_final.withColumn("Année_int", col("Année").cast("int"))
            .orderBy(col("Année_int").asc(), col("Code_Département"))
            .drop("Année_int")
        )

        return self._extracted_from__extracted_from_combine_election_and_orientation_politique_52_116(
            "✅ Transformation des données démographiques terminée", df_final, 5
        )

    def combine_election_and_orientation_politique(self, df_election, df_orientation):
        """
        Combinaison des données électorales avec les données d'orientation politique.
        """
        if df_election is None or df_orientation is None:
            logger.error("❌ Données invalides pour la combinaison")

        logger.info(
            "🚀 Combinaison des données électorales avec les données d'orientation politique..."
        )

        # 3. Nettoyer les noms des candidats
        df_election = df_election.withColumn(
            "candidat_clean", trim(upper(col("candidat")))
        )

        # 4. Mapping candidat -> orientation politique
        candidate_to_orientation = {
            "CHARLES DE GAULLE": "droite",
            "FRANÇOIS MITTERRAND": "gauche",
            "VALÉRY GISCARD D'ESTAING": "centre droite",
            "VALÉRY GISCARD DESTAING": "centre droite",
            "VALERY GISCARD D'ESTAING": "centre droite",
            "VALERY GISCARD DESTAING": "centre droite",
            "JACQUES CHIRAC": "droite",
            "LIONEL JOSPIN": "gauche",
            "NICOLAS SARKOZY": "droite",
            "SÉGOLÈNE ROYAL": "gauche",
            "FRANÇOIS HOLLANDE": "gauche",
            "MARINE LE PEN": "extreme droite",
            "JEAN-LUC MÉLENCHON": "extreme gauche",
            "EMMANUEL MACRON": "centre",
            "ARLETTE LAGUILLER": "extreme gauche",
            "PHILIPPE POUTOU": "extreme gauche",
            "NATHALIE ARTHAUD": "extreme gauche",
            "JEAN-MARIE LE PEN": "extreme droite",
            "BENOÎT HAMON": "gauche",
            "DOMINIQUE DE VILLEPIN": "droite",
            "CHRISTINE BOUTIN": "droite",
            "FRANÇOIS BAYROU": "centre droite",
            "NICOLAS DUPONT-AIGNAN": "droite",
            "ÉRIC ZEMMOUR": "extreme droite",
            "YANNICK JADOT": "écologiste",
            "NOËL MAMÈRE": "écologiste",
            "ANTOINE WAQUIN": "extreme gauche",
            "GEORGES MARCHAIS": "gauche",
            "ROBERT HUE": "gauche",
            "GEORGES POMPIDOU": "droite",
            "ALAIN POHER": "centre droite",
        }

        # 5. Ajouter la colonne orientation politique
        orientation_expr = create_map(
            [lit(k) for k in chain(*candidate_to_orientation.items())]
        )
        df_election = df_election.withColumn(
            "orientation_politique", orientation_expr.getItem(col("candidat_clean"))
        )

        # 6. Créer le mapping orientation -> id à partir du fichier des partis
        orientation_id_map = {
            row["Orientation politique"]: row["id"]
            for row in df_orientation.select("Orientation politique", "id")
            .distinct()
            .collect()
        }
        orientation_id_expr = create_map(
            [lit(k) for k in chain(*orientation_id_map.items())]
        )
        df_election = df_election.withColumn(
            "id_parti", orientation_id_expr.getItem(col("orientation_politique"))
        )

        # Drop candidat_clean
        df_election = df_election.drop("candidat_clean")

        return self._extracted_from_combine_election_and_orientation_politique_52(
            "✅ Combinaison des données électorales avec les données d'orientation politique terminée",
            df_election,
            5,
        )
    
    def prepare_dim_politique(self, df_election):
        """
        Prépare les données pour la dimension politique.
        
        Args:
            df_election: DataFrame des données électorales combinées avec l'orientation politique
        
        Returns:
            DataFrame formaté pour dim_politique
        """
        logger.info("🔄 Préparation des données pour dim_politique")
        
        if df_election is None:
            logger.error("❌ Les données électorales sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_politique = df_election.select(
                col("id_parti").cast(IntegerType()).alias("etiquette_parti"),
                col("annee").cast(IntegerType()).alias("annee"),
                col("code_dept").alias("code_dept"),
                col("candidat").alias("candidat"),
                col("total_voix").cast(IntegerType()).alias("total_voix"),
                col("orientation_politique").alias("orientation_politique")
            )
            
            # Ajout d'un ID unique
            window_spec = Window.orderBy("annee", "code_dept", "candidat")
            dim_politique = dim_politique.withColumn(
                "id", F.row_number().over(window_spec)
            )
            
            # Réorganisation des colonnes pour avoir l'ID en premier
            dim_politique = dim_politique.select(
                "id", "etiquette_parti", "annee", "code_dept", "candidat", "total_voix", "orientation_politique"
            )
            
            # Vérification que code_dept est bien limité à 3 caractères
            dim_politique = dim_politique.withColumn(
                "code_dept",
                F.when(F.length("code_dept") > 3, F.substring("code_dept", 1, 3))
                .otherwise(F.col("code_dept"))
            )
            
            # Afficher un échantillon
            dim_politique.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_politique.count()
            logger.info(f"✅ Dimension politique préparée avec {count_rows} lignes")
            
            return dim_politique
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données politiques: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
            
    def prepare_dim_securite(self, df_securite):
        """
        Prépare les données pour la dimension sécurité.
        
        Args:
            df_securite: DataFrame des données de sécurité (pandas)
        
        Returns:
            DataFrame formaté pour dim_securite
        """
        logger.info("🔄 Préparation des données pour dim_securite")
        
        try:
            # Vérifier si le DataFrame est de type pandas et le convertir en DataFrame Spark
            if isinstance(df_securite, pd.DataFrame):
                logger.info("🔄 Conversion du DataFrame pandas en DataFrame Spark")
                # Prétraiter côté pandas pour éviter problèmes de conversion
                df_securite = df_securite.rename(columns={
                    'Année': 'annee',
                    'Département': 'code_dept',
                    'Délits_total': 'delits_total'
                })
                
                # Sélection des colonnes nécessaires uniquement
                df_securite = df_securite[['annee', 'code_dept', 'delits_total']]
                
                # Conversion en types appropriés avant passage à Spark
                df_securite['annee'] = df_securite['annee'].astype(int)
                df_securite['delits_total'] = df_securite['delits_total'].astype(int)
                df_securite['code_dept'] = df_securite['code_dept'].astype(str)
                
                # S'assurer que code_dept ne dépasse pas 3 caractères
                df_securite['code_dept'] = df_securite['code_dept'].apply(
                    lambda x: x[:3] if len(x) > 3 else x
                )
                
                # Création du schéma Spark explicite
                schema = StructType([
                    StructField("annee", IntegerType(), False),
                    StructField("code_dept", StringType(), False),
                    StructField("delits_total", IntegerType(), False)
                ])
                
                # Conversion en DataFrame Spark
                spark = SparkSession.builder.getOrCreate()
                dim_securite = spark.createDataFrame(df_securite, schema=schema)
            else:
                # Si c'est déjà un DataFrame Spark
                dim_securite = df_securite.select(
                    col("annee").cast(IntegerType()).alias("annee"),
                    col("code_dept").alias("code_dept"),
                    col("delits_total").cast(IntegerType()).alias("delits_total")
                )
                
            # Vérifier que code_dept est limité à 3 caractères
            dim_securite = dim_securite.withColumn(
                "code_dept",
                F.when(F.length("code_dept") > 3, F.substring("code_dept", 1, 3))
                .otherwise(F.col("code_dept"))
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon des données (éviter show() qui a causé un crash)
            logger.info(f"✅ Dimension sécurité préparée avec succès")
            
            return dim_securite
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données de sécurité: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_dim_sante(self, df_life):
        """
        Prépare les données pour la dimension santé.
        
        Args:
            df_life: DataFrame des données d'espérance de vie
        
        Returns:
            DataFrame formaté pour dim_sante
        """
        logger.info("🔄 Préparation des données pour dim_sante")
        
        if df_life is None:
            logger.error("❌ Les données d'espérance de vie sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_sante = df_life.select(
                col("CODE_DEP").alias("code_dept"),
                col("Année").cast(IntegerType()).alias("annee"),
                col("Espérance_Vie").cast(DoubleType()).alias("esperance_vie")
            )
            
            # Vérifier que code_dept est limité à 3 caractères
            dim_sante = dim_sante.withColumn(
                "code_dept",
                F.when(F.length("code_dept") > 3, F.substring("code_dept", 1, 3))
                .otherwise(F.col("code_dept"))
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon
            dim_sante.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_sante.count()
            logger.info(f"✅ Dimension santé préparée avec {count_rows} lignes")
            
            return dim_sante
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données de santé: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_dim_education(self, df_education):
        """
        Prépare les données pour la dimension éducation.
        
        Args:
            df_education: DataFrame des données d'éducation
        
        Returns:
            DataFrame formaté pour dim_education
        """
        logger.info("🔄 Préparation des données pour dim_education")
        
        if df_education is None:
            logger.error("❌ Les données d'éducation sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_education = df_education.select(
                col("code_departement").alias("code_departement"),
                col("annee_fermeture").cast(IntegerType()).alias("annee_fermeture"),
                col("libelle_departement").alias("libelle_departement"),
                col("nombre_total_etablissements").cast(IntegerType()).alias("nombre_total_etablissements"),
                col("nb_public").cast(IntegerType()).alias("nb_public"),
                col("nb_prive").cast(IntegerType()).alias("nb_prive"),
                col("pct_public").cast(DoubleType()).alias("pct_public"),
                col("pct_prive").cast(DoubleType()).alias("pct_prive")
            )
            
            # Vérifier que code_departement est limité à 3 caractères
            dim_education = dim_education.withColumn(
                "code_departement",
                F.when(F.length("code_departement") > 3, F.substring("code_departement", 1, 3))
                .otherwise(F.col("code_departement"))
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon
            dim_education.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_education.count()
            logger.info(f"✅ Dimension éducation préparée avec {count_rows} lignes")
            
            return dim_education
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données d'éducation: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_dim_environnement(self, df_env):
        """
        Prépare les données pour la dimension environnement.
        
        Args:
            df_env: DataFrame des données environnementales
        
        Returns:
            DataFrame formaté pour dim_environnement
        """
        logger.info("🔄 Préparation des données pour dim_environnement")
        
        if df_env is None:
            logger.error("❌ Les données environnementales sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_environnement = df_env.select(
                col("Code_INSEE_Région").alias("code_insee_region"),
                col("Année").cast(IntegerType()).alias("annee"),
                col("Parc_installé_éolien_MW").cast(DoubleType()).alias("parc_eolien_mw"),
                col("Parc_installé_solaire_MW").cast(DoubleType()).alias("parc_solaire_mw")
            )
            
            # Vérifier que code_insee_region est limité à 3 caractères
            dim_environnement = dim_environnement.withColumn(
                "code_insee_region",
                F.when(F.length("code_insee_region") > 3, F.substring("code_insee_region", 1, 3))
                .otherwise(F.col("code_insee_region"))
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon
            dim_environnement.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_environnement.count()
            logger.info(f"✅ Dimension environnement préparée avec {count_rows} lignes")
            
            return dim_environnement
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données environnementales: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_dim_socio_economie(self, df_pib_inflation):
        """
        Prépare les données pour la dimension socio-économie.
        
        Args:
            df_pib_inflation: DataFrame des données PIB et inflation
        
        Returns:
            DataFrame formaté pour dim_socio_economie
        """
        logger.info("🔄 Préparation des données pour dim_socio_economie")
        
        if df_pib_inflation is None:
            logger.error("❌ Les données PIB et inflation sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_socio_economie = df_pib_inflation.select(
                col("Année").cast(IntegerType()).alias("annee"),
                col("PIB_en_euros_par_habitant").cast(DoubleType()).alias("pib_euros_par_habitant"),
                col("Code_INSEE_Région").alias("code_insee_region"),
                col("Évolution_des_prix_à_la_consommation").cast(DoubleType()).alias("evolution_prix_conso"),
                col("PIB_par_inflation").cast(DoubleType()).alias("pib_par_inflation")
            )
            
            # Vérifier que code_insee_region est limité à 3 caractères
            dim_socio_economie = dim_socio_economie.withColumn(
                "code_insee_region",
                F.when(F.length("code_insee_region") > 3, F.substring("code_insee_region", 1, 3))
                .otherwise(F.col("code_insee_region"))
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon
            dim_socio_economie.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_socio_economie.count()
            logger.info(f"✅ Dimension socio-économie préparée avec {count_rows} lignes")
            
            return dim_socio_economie
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données socio-économiques: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_dim_technologie(self, df_tech):
        """
        Prépare les données pour la dimension technologie.
        
        Args:
            df_tech: DataFrame des données technologiques
        
        Returns:
            DataFrame formaté pour dim_technologie
        """
        logger.info("🔄 Préparation des données pour dim_technologie")
        
        if df_tech is None:
            logger.error("❌ Les données technologiques sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_technologie = df_tech.select(
                col("annee").cast(IntegerType()).alias("annee"),
                col("dird_pib_france_pourcentages").cast(DoubleType()).alias("depenses_rd_pib")
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon
            dim_technologie.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_technologie.count()
            logger.info(f"✅ Dimension technologie préparée avec {count_rows} lignes")
            
            return dim_technologie
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données technologiques: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_dim_demographie(self, df_demo):
        """
        Prépare les données pour la dimension démographie.
        
        Args:
            df_demo: DataFrame des données démographiques
        
        Returns:
            DataFrame formaté pour dim_demographie
        """
        logger.info("🔄 Préparation des données pour dim_demographie")
        
        if df_demo is None:
            logger.error("❌ Les données démographiques sont None")
            return None
            
        try:
            # Sélection et renommage des colonnes nécessaires
            dim_demographie = df_demo.select(
                col("Année").cast(IntegerType()).alias("annee"),
                col("Code_Département").alias("code_departement"),
                col("Nom_Département").alias("nom_departement"),
                col("E_Total").cast(IntegerType()).alias("population_totale"),
                col("H_Total").cast(IntegerType()).alias("population_hommes"),
                col("F_Total").cast(IntegerType()).alias("population_femmes"),
                col("E_0_19_ans").cast(IntegerType()).alias("pop_0_19"),
                col("E_20_39_ans").cast(IntegerType()).alias("pop_20_39"),
                col("E_40_59_ans").cast(IntegerType()).alias("pop_40_59"),
                col("E_60_74_ans").cast(IntegerType()).alias("pop_60_74"),
                col("E_75_et_plus").cast(IntegerType()).alias("pop_75_plus")
            )
            
            # Vérifier que code_departement est limité à 3 caractères
            dim_demographie = dim_demographie.withColumn(
                "code_departement",
                F.when(F.length("code_departement") > 3, F.substring("code_departement", 1, 3))
                .otherwise(F.col("code_departement"))
            )
            
            # L'ID sera attribué automatiquement par MySQL (AUTO_INCREMENT)
            
            # Afficher un échantillon
            dim_demographie.show(5, truncate=False)
            
            # Récupérer le nombre de lignes
            count_rows = dim_demographie.count()
            logger.info(f"✅ Dimension démographie préparée avec {count_rows} lignes")
            
            return dim_demographie
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation des données démographiques: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def prepare_fact_resultats_politique(self, dim_politique, dim_securite, dim_socio_economie, 
                                        dim_sante, dim_environnement, dim_education, 
                                        dim_demographie, dim_technologie):
        """
        Prépare la table de faits qui relie toutes les dimensions.
        
        Args:
            dim_politique: DataFrame de la dimension politique
            dim_securite: DataFrame de la dimension sécurité
            dim_socio_economie: DataFrame de la dimension socio-économie
            dim_sante: DataFrame de la dimension santé
            dim_environnement: DataFrame de la dimension environnement
            dim_education: DataFrame de la dimension éducation
            dim_demographie: DataFrame de la dimension démographie
            dim_technologie: DataFrame de la dimension technologie
        
        Returns:
            DataFrame formaté pour fact_resultats_politique
        """
        logger.info("🔄 Préparation de la table de faits resultats_politique")
        
        try:
            # On part de la dimension politique comme base
            if dim_politique is None:
                logger.error("❌ La dimension politique est None, impossible de créer la table de faits")
                return None
                
            # Créer une clé unique annee_code_dpt pour la table de faits
            fact_table = dim_politique.withColumn(
                "annee_code_dpt", 
                F.concat(F.col("annee").cast("string"), F.lit("_"), F.col("code_dept"))
            )
            
            # Sélectionner les colonnes nécessaires pour la table de faits
            fact_table = fact_table.select(
                "annee_code_dpt", 
                col("id").alias("id_parti"),  # id de la dimension politique
                col("annee"),
                col("code_dept")
            )
            
            # Intégrer les ID des autres dimensions
            
            # Jointure avec dimension sécurité
            if dim_securite is not None:
                logger.info("🔄 Jointure avec dimension sécurité")
                fact_table = fact_table.join(
                    dim_securite.select(
                        F.monotonically_increasing_id().alias("securite_id"), 
                        col("annee").alias("sec_annee"), 
                        col("code_dept").alias("sec_code_dept")
                    ),
                    (fact_table.annee == F.col("sec_annee")) & 
                    (fact_table.code_dept == F.col("sec_code_dept")),
                    "left"
                ).drop("sec_annee", "sec_code_dept")
            else:
                fact_table = fact_table.withColumn("securite_id", F.lit(None).cast(IntegerType()))
                
            # Jointure avec dimension socio-économie (niveau région)
            if dim_socio_economie is not None:
                logger.info("🔄 Jointure avec dimension socio-économie")
                # Pour simplifier, on utilise l'année uniquement car les données sont au niveau régional
                # Une approche plus précise nécessiterait une table de correspondance département-région
                fact_table = fact_table.join(
                    dim_socio_economie.select(
                        F.monotonically_increasing_id().alias("socio_eco_id"), 
                        col("annee").alias("eco_annee")
                    ),
                    (fact_table.annee == F.col("eco_annee")),
                    "left"
                ).drop("eco_annee")
            else:
                fact_table = fact_table.withColumn("socio_eco_id", F.lit(None).cast(IntegerType()))
                
            # Jointure avec dimension santé
            if dim_sante is not None:
                logger.info("🔄 Jointure avec dimension santé")
                fact_table = fact_table.join(
                    dim_sante.select(
                        F.monotonically_increasing_id().alias("sante_id"), 
                        col("annee").alias("sante_annee"), 
                        col("code_dept").alias("sante_code_dept")
                    ),
                    (fact_table.annee == F.col("sante_annee")) & 
                    (fact_table.code_dept == F.col("sante_code_dept")),
                    "left"
                ).drop("sante_annee", "sante_code_dept")
            else:
                fact_table = fact_table.withColumn("sante_id", F.lit(None).cast(IntegerType()))
                
            # Jointure avec dimension environnement (niveau région)
            if dim_environnement is not None:
                logger.info("🔄 Jointure avec dimension environnement")
                # Pour simplifier, on utilise l'année uniquement car les données sont au niveau régional
                fact_table = fact_table.join(
                    dim_environnement.select(
                        F.monotonically_increasing_id().alias("environnement_id"), 
                        col("annee").alias("env_annee")
                    ),
                    (fact_table.annee == F.col("env_annee")),
                    "left"
                ).drop("env_annee")
            else:
                fact_table = fact_table.withColumn("environnement_id", F.lit(None).cast(IntegerType()))
                
            # Jointure avec dimension éducation
            if dim_education is not None:
                logger.info("🔄 Jointure avec dimension éducation")
                fact_table = fact_table.join(
                    dim_education.select(
                        F.monotonically_increasing_id().alias("education_id"), 
                        col("annee_fermeture").alias("edu_annee"), 
                        col("code_departement").alias("edu_code_dept")
                    ),
                    (fact_table.annee == F.col("edu_annee")) & 
                    (fact_table.code_dept == F.col("edu_code_dept")),
                    "left"
                ).drop("edu_annee", "edu_code_dept")
            else:
                fact_table = fact_table.withColumn("education_id", F.lit(None).cast(IntegerType()))
                
            # Jointure avec dimension démographie
            if dim_demographie is not None:
                logger.info("🔄 Jointure avec dimension démographie")
                fact_table = fact_table.join(
                    dim_demographie.select(
                        F.monotonically_increasing_id().alias("demographie_id"), 
                        col("annee").alias("demo_annee"), 
                        col("code_departement").alias("demo_code_dept")
                    ),
                    (fact_table.annee == F.col("demo_annee")) & 
                    (fact_table.code_dept == F.col("demo_code_dept")),
                    "left"
                ).drop("demo_annee", "demo_code_dept")
            else:
                fact_table = fact_table.withColumn("demographie_id", F.lit(None).cast(IntegerType()))
                
            # Jointure avec dimension technologie (niveau national)
            if dim_technologie is not None:
                logger.info("🔄 Jointure avec dimension technologie")
                fact_table = fact_table.join(
                    dim_technologie.select(
                        F.monotonically_increasing_id().alias("technologie_id"), 
                        col("annee").alias("tech_annee")
                    ),
                    (fact_table.annee == F.col("tech_annee")),
                    "left"
                ).drop("tech_annee")
            else:
                fact_table = fact_table.withColumn("technologie_id", F.lit(None).cast(IntegerType()))
            
            # Sélectionner les colonnes finales pour la table de faits
            fact_table = fact_table.select(
                "annee_code_dpt",
                "id_parti",
                "securite_id",
                "socio_eco_id",
                "sante_id",
                "environnement_id",
                "education_id",
                "demographie_id",
                "technologie_id"
            )
            
            # IMPORTANT: Nous utilisons count() au lieu de show() pour éviter les problèmes d'affichage
            count_rows = fact_table.count()
            logger.info(f"✅ Table de faits préparée avec {count_rows} lignes")
            
            return fact_table
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la préparation de la table de faits: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    # TODO Rename this here and in `transform_environmental_data`, `transform_pib_outre_mer`, `fill_missing_pib_mayotte`, `combine_all_pib_data`, `transform_inflation_data`, `combine_pib_and_inflation`, `transform_education_data`, `calculate_closed_by_year_and_dept_education`, `transform_demography_data` and `combine_election_and_orientation_politique`
    def _extracted_from_combine_election_and_orientation_politique_52(
        self, arg0, arg1, arg2
    ):
        return self._extracted_from__extracted_from_combine_election_and_orientation_politique_52_116(
            arg0, arg1, arg2
        )

    # TODO Rename this here and in `calculate_closed_by_year_and_dept_education`, `transform_demography_data` and `_extracted_from_combine_election_and_orientation_politique_52`
    def _extracted_from__extracted_from_combine_election_and_orientation_politique_52_116(self, arg0, arg1, arg2):
        logger.info(arg0)
        arg1.show(arg2, truncate=False)
        return arg1

    # TODO Rename this here and in `calculate_closed_by_year_and_dept_education`, `transform_demography_data` and `_extracted_from_combine_election_and_orientation_politique_52`
    def _extracted_from__extracted_from_combine_election_and_orientation_politique_52_116(self, arg0, arg1, arg2):
        logger.info(arg0)
        arg1.show(arg2, truncate=False)
        return arg1
