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
from pyspark.sql.functions import col

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

    def stop(self):
        """
        Arrête la session Spark si elle est active.
        """
        if self.spark:
            self.spark.stop()
            logger.info("🛑 Session Spark arrêtée proprement.")
