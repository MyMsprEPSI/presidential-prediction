# transform.py

import logging
from pyspark.sql.functions import col, when, lit, isnan, sum as spark_sum, round
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


# Configuration du logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataTransformer:
    """
    Classe permettant de transformer les données extraites avant leur chargement.
    """

    def __init__(self):
        pass


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
        df_transformed = df_transformed.fillna({"Parc_installé_éolien_MW": 0.0, "Parc_installé_solaire_MW": 0.0})

        # Regroupement par région et année pour sommer les valeurs en cas de doublons
        df_grouped = df_transformed.groupBy("Code_INSEE_Région", "Année").agg(
            spark_sum("Parc_installé_éolien_MW").alias("Parc_installé_éolien_MW"),
            spark_sum("Parc_installé_solaire_MW").alias("Parc_installé_solaire_MW"),
        )

        # Récupération des régions uniques présentes dans les données
        regions = df_grouped.select("Code_INSEE_Région").distinct()

        # Création d'un DataFrame contenant l'année 2000 pour chaque région avec valeurs à 0.0
        df_year_2000 = regions.withColumn("Année", lit(2000)).withColumn("Parc_installé_éolien_MW", lit(0.0)).withColumn("Parc_installé_solaire_MW", lit(0.0))

        # Ajout des données de l'année 2000 au DataFrame principal
        df_final = df_grouped.union(df_year_2000)

        # Tri des données par région et année
        df_final = df_final.orderBy("Code_INSEE_Région", "Année")

        logger.info("✅ Transformation terminée ! Aperçu des données transformées :")
        df_final.show(15, truncate=False)

        return df_final


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
                condition = when(
                    col("source_file") == file_path, lit(code_region)
                )
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

        logger.info("✅ Transformation PIB terminée ! Aperçu des données transformées :")
        df_final.show(10, truncate=False)

        return df_final

    def fill_missing_pib_mayotte(self, df_pib):
        """
        Remplit les valeurs manquantes du PIB de Mayotte par régression linéaire.
        """

        from pyspark.sql.functions import col

        logger.info("🚀 Remplissage des valeurs manquantes PIB Mayotte en cours...")

        df_mayotte = df_pib.filter(col("Code_INSEE_Région") == "06")

        known_data = df_mayotte.filter(col("PIB_en_euros_par_habitant").isNotNull())
        unknown_data = df_mayotte.filter(col("PIB_en_euros_par_habitant").isNull())

        assembler = VectorAssembler(inputCols=["Année"], outputCol="features")
        train_data = assembler.transform(known_data).select(
            "features", "PIB_en_euros_par_habitant"
        )

        # Modèle de régression linéaire
        lr = LinearRegression(featuresCol="features", labelCol="PIB_en_euros_par_habitant")
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

        logger.info("✅ Remplissage PIB Mayotte terminé :")
        df_final.show(10, truncate=False)

        return df_final

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

        logger.info("✅ Fusion des données PIB réussie :")
        df_final.show(10, truncate=False)

        return df_final
    
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

        logger.info("✅ Transformation des données d'inflation réussie :")
        df_transformed.show(10, truncate=False)

        return df_transformed




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
            "Évolution_des_prix_à_la_consommation", round(col("Évolution_des_prix_à_la_consommation"), 2)
        )

        df_combined = df_combined.withColumn(
            "PIB_par_inflation",
            round(
                col("PIB_en_euros_par_habitant") / (1 + col("Évolution_des_prix_à_la_consommation") / 100), 2
            )
        )

        # Trier les résultats
        df_combined = df_combined.orderBy("Code_INSEE_Région", "Année")

        logger.info("✅ Fusion des données PIB et Inflation réussie :")
        df_combined.show(10, truncate=False)

        return df_combined




