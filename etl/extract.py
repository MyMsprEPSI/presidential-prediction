# extract.py

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import col, lit

# Configuration du logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataExtractor:
    """
    Classe permettant d'extraire les données des fichiers CSV pour l'ETL.
    """

    def __init__(self, app_name="EnvironmentalDataETL", master="local[*]"):
        """
        Initialise une session Spark et définit les paramètres de logging.
        """
        self.spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.driver.host", "127.0.0.1")
            .config(
                "spark.driver.extraClassPath",
                "./database/connector/mysql-connector-j-9.1.0.jar;./database/connector/spark-excel_2.12-3.5.0_0.20.3.jar",
            )
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .getOrCreate()
        )

        # Réduction des logs Spark pour éviter le bruit
        self.spark.sparkContext.setLogLevel("ERROR")

    def extract_environmental_data(self, file_path):
        """
        Charge les données du fichier "parc-regional-annuel-prod-eolien-solaire.csv"
        et les retourne sous forme de DataFrame PySpark.

        :param file_path: Chemin du fichier CSV à charger
        :return: DataFrame PySpark contenant les données environnementales
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ Fichier non trouvé : {file_path}")
            return None

        logger.info(f"📥 Extraction des données environnementales depuis : {file_path}")

        # Définition du schéma du fichier
        schema = StructType(
            [
                StructField("Année", IntegerType(), True),
                StructField("Code INSEE région", IntegerType(), True),
                StructField("Région", StringType(), True),
                StructField("Parc installé éolien (MW)", DoubleType(), True),
                StructField("Parc installé solaire (MW)", DoubleType(), True),
                StructField("Géo-shape région", StringType(), True),
                StructField("Géo-point région", StringType(), True),
            ]
        )

        # Chargement du fichier CSV avec gestion des erreurs
        try:
            df = (
                self.spark.read.option("header", "true")
                .option("delimiter", ";")
                .option("enforceSchema", "true")  # Assure que le schéma est respecté
                .schema(schema)
                .csv(file_path)
            )

            # Normalisation des noms de colonnes (évite les erreurs de mapping)
            df = (
                df.withColumnRenamed("Code INSEE région", "Code_INSEE_région")
                .withColumnRenamed(
                    "Parc installé éolien (MW)", "Parc_installé_éolien_MW"
                )
                .withColumnRenamed(
                    "Parc installé solaire (MW)", "Parc_installé_solaire_MW"
                )
                .withColumnRenamed("Géo-shape région", "Géo_shape_région")
                .withColumnRenamed("Géo-point région", "Géo_point_région")
            )

            return df

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'extraction des données : {str(e)}")
            return None

    def extract_pib_outre_mer(self, file_paths):
        """
        Extrait et combine les données PIB brutes des fichiers CSV outre-mer.
        """
        schema = StructType(
            [
                StructField("Année", StringType(), True),
                StructField("PIB_en_euros_par_habitant", StringType(), True),
                StructField("Codes", StringType(), True),
            ]
        )

        dfs = []
        for path in file_paths:
            if os.path.exists(path):
                df = (
                    self.spark.read.option("header", "true")
                    .option("delimiter", ";")
                    .schema(schema)
                    .csv(path)
                )
                # Ajoute une colonne temporaire indiquant le fichier source
                df = df.withColumn("source_file", lit(path))
                dfs.append(df)
            else:
                logger.error(f"❌ Fichier non trouvé : {path}")

        if not dfs:
            logger.error("❌ Aucun fichier PIB valide trouvé.")
            return None

        from functools import reduce

        df_combined = reduce(lambda df1, df2: df1.union(df2), dfs)

        return df_combined

    def extract_pib_excel(self, excel_path):
        """
        Extrait les données PIB régionales à partir du fichier Excel (1990-2021).
        """
        if not os.path.exists(excel_path):
            logger.error(f"❌ Fichier non trouvé : {excel_path}")
            return None

        logger.info(f"📥 Extraction PIB Excel : {excel_path}")

        schema = StructType(
            [
                StructField("Code_INSEE_Région", StringType(), True),
                StructField("libgeo", StringType(), True),
                StructField("Année", IntegerType(), True),
                StructField("PIB_en_euros_par_habitant", IntegerType(), True),
            ]
        )

        try:
            df = (
                self.spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("dataAddress", "'Data'!A5")
                .schema(schema)
                .load(excel_path)
            )

            df = df.select("Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région")
            return df

        except Exception as e:
            logger.error(f"❌ Erreur extraction Excel : {str(e)}")
            return None

    def extract_pib_2022(self, csv_path):
        """
        Extrait et nettoie le fichier PIB 2022 régional CSV.
        """

        if not os.path.exists(csv_path):
            logger.error(f"❌ Fichier non trouvé : {csv_path}")
            return None

        logger.info(f"📥 Extraction PIB 2022 depuis : {csv_path}")

        # Définition du schéma correct
        schema = StructType(
            [
                StructField("Code_INSEE_Région", StringType(), True),
                StructField("Libellé", StringType(), True),
                StructField("PIB_en_euros_par_habitant", IntegerType(), True),
            ]
        )

        # Lecture du fichier en ignorant les 2 premières lignes inutiles
        df_raw = (
            self.spark.read.option("header", "true")
            .option("delimiter", ";")
            .option("inferSchema", "true")  # Permet d'autodétecter les types
            .schema(schema)  # Appliquer le schéma explicite
            .csv(csv_path)
        )

        # Vérification des colonnes détectées
        logger.info(f"🛠️ Colonnes après nettoyage : {df_raw.columns}")

        # Ajout de l'année 2022 à chaque ligne
        df_cleaned = df_raw.select(
            col("Code_INSEE_Région"),
            lit(2022).alias("Année"),
            col("PIB_en_euros_par_habitant")
        )

        return df_cleaned

    def stop(self):
        """
        Arrête la session Spark si elle est active.
        """
        if self.spark:
            self.spark.stop()
            logger.info("🛑 Session Spark arrêtée proprement.")
