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
from pyspark.sql.functions import col, lit, regexp_extract
import glob

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
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "8g")
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
            col("PIB_en_euros_par_habitant"),
        )

        return df_cleaned

    def extract_inflation_data(self, excel_path):
        """
        Extrait les données d'inflation depuis le fichier Excel.

        :param excel_path: Chemin du fichier Excel contenant les données d'inflation.
        :return: DataFrame PySpark contenant les données d'inflation filtrées de 2000 à 2022.
        """
        if not os.path.exists(excel_path):
            logger.error(f"❌ Fichier non trouvé : {excel_path}")
            return None

        logger.info(f"📥 Extraction des données d'inflation depuis : {excel_path}")

        # Définition du schéma explicitement pour correspondre à la ligne d'en-tête effective
        schema = StructType(
            [
                StructField("Année", IntegerType(), True),
                StructField("Évolution des prix à la consommation", DoubleType(), True),
            ]
        )

        try:
            # On spécifie la feuille et la cellule de départ (ici A4, supposé contenir les en-têtes)
            df = (
                self.spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("sheetName", "Question 1")
                .option("dataAddress", "'Question 1'!A4")
                .schema(schema)
                .load(excel_path)
            )

            # Pour plus de sécurité, renomme la colonne afin de supprimer les espaces
            df = df.withColumnRenamed(
                "Évolution des prix à la consommation",
                "Évolution_des_prix_à_la_consommation",
            )

            logger.info(f"🛠️ Colonnes après extraction et renommage : {df.columns}")

            # Filtrer les années de 2000 à 2022
            df_filtered = df.filter((col("Année") >= 2000) & (col("Année") <= 2022))

            logger.info("✅ Extraction des données d'inflation réussie :")
            df_filtered.show(10, truncate=False)

            return df_filtered

        except Exception as e:
            logger.error(f"❌ Erreur extraction Excel inflation : {str(e)}")
            return None

    def extract_technologie_data(self, excel_path):
        """
        Extrait les données de technologie depuis le fichier Excel.

        :param excel_path: Chemin du fichier Excel contenant les données de technologie
        :return: DataFrame PySpark contenant les données brutes
        """
        if not os.path.exists(excel_path):
            logger.error(f"❌ Fichier non trouvé : {excel_path}")
            return None

        logger.info(f"📥 Extraction des données de technologie depuis : {excel_path}")

        try:
            df = (
                self.spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dataAddress", "'Tableau 1'!A3:B37")
                .load(excel_path)
            )

            logger.info("✅ Extraction des données de technologie réussie")
            return df

        except Exception as e:
            logger.error(
                f"❌ Erreur lors de l'extraction des données de technologie : {str(e)}"
            )
            return None

    def extract_election_data_1965_2012(self, file_pattern):
        """
        Extrait les données électorales des fichiers CSV de 1965 à 2012.

        :param file_pattern: Motif des fichiers CSV à traiter (glob)
        :return: Liste de DataFrames PySpark contenant les données brutes
        """
        logger.info(f"📥 Extraction des données électorales 1965-2012 depuis : {file_pattern}")
        file_list = glob.glob(file_pattern)
        results = []

        for file_path in file_list:
            try:
                # Lecture du fichier CSV avec en-tête et schéma inféré
                df = (
                    self.spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(file_path)
                )

                # Ajout des colonnes année et nom de fichier
                df = df.withColumn("filename", lit(file_path))
                df = df.withColumn(
                    "annee", regexp_extract("filename", r"presi(\d{4})", 1)
                )

                results.append(df)
            except Exception as e:
                logger.error(f"❌ Erreur lors de l'extraction du fichier {file_path}: {str(e)}")
                continue

        if not results:
            logger.warning("Aucun fichier CSV trouvé pour 1965-2012.")
            return None

        return results

    def extract_election_data_2017(self, excel_file):
        """
        Extrait les données électorales du fichier Excel 2017.
        """
        logger.info(f"📥 Extraction des données électorales 2017 : {excel_file}")
        try:
            return (
                self.spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("dataAddress", "'Départements Tour 2'!A3:Z115")
                .load(excel_file)
            )
        except Exception as e:
            logger.error(f"❌ Erreur extraction 2017 : {str(e)}")
            return None

    def extract_election_data_2022(self, excel_file):
        """
        Extrait les données électorales du fichier Excel 2022.
        """
        logger.info(f"📥 Extraction des données électorales 2022 : {excel_file}")
        try:
            return (
                self.spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sheetName", "Résultats")
                .load(excel_file)
            )
        except Exception as e:
            logger.error(f"❌ Erreur extraction 2022 : {str(e)}")
            return None

    def stop(self):
        """
        Arrête la session Spark si elle est active.
        """
        if self.spark:
            self.spark.stop()
            logger.info("🛑 Session Spark arrêtée proprement.")
