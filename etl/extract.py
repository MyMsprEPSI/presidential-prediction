# extract.py

import os
import pandas as pd
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
import logging
import traceback
from functools import reduce


# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def convert_excel_to_xlsx(file_xls, file_xlsx):
    """
    Convertit le fichier XLS en XLSX si ce dernier n'existe pas déjà.
    """
    if not os.path.exists(file_xlsx):
        print("📥 Conversion du fichier XLS en XLSX...")
        try:
            # Charger toutes les feuilles du fichier XLS
            excel_data = pd.read_excel(file_xls, sheet_name=None)
            with pd.ExcelWriter(file_xlsx, engine="openpyxl") as writer:
                for sheet, data in excel_data.items():
                    data.to_excel(writer, sheet_name=sheet, index=False)
            print(f"✅ Conversion réussie : {file_xlsx}")
        except Exception as e:
            print(f"❌ Erreur lors de la conversion : {e}")
    else:
        print(f"✓ {file_xlsx} existe déjà. Conversion ignorée.")


def extract_and_merge_excel(file_xlsx, file_csv):
    """
    Extrait et fusionne les feuilles Excel (sauf la première) en un unique CSV.
    Les feuilles sont lues en ignorant les trois premières lignes (skiprows=3),
    et une colonne "Année" est ajoutée à partir du nom de la feuille.
    Le CSV est écrit avec le séparateur ';'.
    """
    if not os.path.exists(file_csv):
        print("📥 Extraction et fusion des feuilles Excel...")
        try:
            df_final = _process_excel_sheets(file_xlsx)
            # Sauvegarder en CSV avec le séparateur point-virgule
            df_final.to_csv(file_csv, index=False, sep=";")
            print(f"✅ Fichier CSV généré : {file_csv}")
        except Exception as e:
            print(f"❌ Erreur lors de l'extraction/fusion : {e}")
    else:
        print(f"✓ {file_csv} existe déjà. Extraction ignorée.")
    
    
def _process_excel_sheets(file_xlsx):
    """
    Traite les feuilles Excel en ignorant la première feuille (ex: "À savoir").
    Retourne un DataFrame combiné avec une colonne Année ajoutée.
    """
    # Charger le classeur XLSX
    xls = pd.ExcelFile(file_xlsx)
    # On ignore la première feuille (ex: "À savoir")
    sheets_to_read = xls.sheet_names[1:]
    dfs = []
    for sheet in sheets_to_read:
        print(f"📄 Traitement de la feuille : {sheet}")
        df = pd.read_excel(xls, sheet_name=sheet, skiprows=3)
        df["Année"] = sheet  # Ajouter l'année issue du nom de la feuille
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


class DataExtractor:
    """
    Classe permettant d'extraire les données des fichiers CSV pour l'ETL.
    """

    def __init__(self, app_name="EnvironmentalDataETL", master="local[*]"):
        """Initialise une session Spark."""
        logger.info("🚀 Initialisation de la session Spark...")
        self.spark = self._create_spark_session(app_name, master)
        self.spark.sparkContext.setLogLevel("ERROR")  # Réduction des logs Spark
        logger.info("✅ Session Spark initialisée avec succès")

    def _create_spark_session(self, app_name, master):
        """Crée et configure une session Spark."""
        return (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.driver.host", "127.0.0.1")
            .config(
                "spark.python.worker.reuse", "true"
            )  # Réutiliser les workers Python
            .config("spark.python.worker.timeout", "1800")  # Augmenter le timeout
            .config(
                "spark.driver.extraClassPath",
                "./database/connector/mysql-connector-j-9.1.0.jar;./database/connector/spark-excel_2.12-3.5.0_0.20.3.jar",
            )
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "8g")
            # Ajouter le chemin Python explicite
            .config(
                "spark.pyspark.python", "python"
            )  # ou le chemin complet vers votre exécutable Python
            .config("spark.pyspark.driver.python", "python")  # ou le chemin complet
            .getOrCreate()
        )

    def extract_environmental_data(self, file_path):
        """
        Charge les données du fichier "parc-regional-annuel-prod-eolien-solaire.csv".

        :param file_path: Chemin du fichier CSV à charger
        :return: DataFrame PySpark contenant les données environnementales
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ Fichier non trouvé : {file_path}")
            return None

        logger.info(f"📌 Extraction des données environnementales depuis : {file_path}")

        try:
            df = self._load_environmental_data(file_path)
            df = self._normalize_column_names(df)
            return df
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'extraction des données : {str(e)}")
            return None

    def _load_environmental_data(self, file_path):
        """Charge le fichier CSV avec un schéma spécifique."""
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

        return (
            self.spark.read.option("header", "true")
            .option("delimiter", ";")
            .option("enforceSchema", "true")
            .schema(schema)
            .csv(file_path)
        )

    def _normalize_column_names(self, df):
        """Normalise les noms de colonnes."""
        return (
            df.withColumnRenamed("Code INSEE région", "Code_INSEE_région")
            .withColumnRenamed("Parc installé éolien (MW)", "Parc_installé_éolien_MW")
            .withColumnRenamed("Parc installé solaire (MW)", "Parc_installé_solaire_MW")
            .withColumnRenamed("Géo-shape région", "Géo_shape_région")
            .withColumnRenamed("Géo-point région", "Géo_point_région")
        )

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

        return reduce(lambda df1, df2: df1.union(df2), dfs)

    def extract_pib_excel(self, excel_path):
        """
        Extrait les données PIB régionales à partir du fichier Excel (1990-2021).
        """
        if not os.path.exists(excel_path):
            logger.error(f"❌ Fichier non trouvé : {excel_path}")
            return None

        logger.info(f"📥 Extraction PIB Excel : {excel_path}")

        try:
            df = self._load_pib_excel_data(excel_path)
            df = self._select_pib_columns(df)
            return df
        except Exception as e:
            logger.error(f"❌ Erreur extraction Excel : {str(e)}")
            return None

    def _load_pib_excel_data(self, excel_path):
        """Charge les données PIB depuis le fichier Excel avec un schéma défini."""
        schema = StructType(
            [
                StructField("Code_INSEE_Région", StringType(), True),
                StructField("libgeo", StringType(), True),
                StructField("Année", IntegerType(), True),
                StructField("PIB_en_euros_par_habitant", IntegerType(), True),
            ]
        )
        return (
            self.spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("dataAddress", "'Data'!A5")
            .schema(schema)
            .load(excel_path)
        )

    def _select_pib_columns(self, df):
        """Sélectionne les colonnes pertinentes pour les données PIB."""
        return df.select("Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région")

    def extract_pib_2022(self, csv_path):
        """
        Extrait le fichier PIB 2022 régional CSV.
        """

        if not os.path.exists(csv_path):
            logger.error(f"❌ Fichier non trouvé : {csv_path}")
            return None

        logger.info(f"📥 Extraction PIB 2022 depuis : {csv_path}")

        try:
            df_raw = self._load_pib_2022_data(csv_path)
            return self._select_and_rename_pib_2022_columns(df_raw)
        except Exception as e:
            logger.error(f"❌ Erreur extraction PIB 2022: {str(e)}")
            return None

    def _load_pib_2022_data(self, csv_path):
        """Charge les données PIB 2022 depuis le fichier CSV avec un schéma défini."""
        schema = StructType(
            [
                StructField("Code_INSEE_Région", StringType(), True),
                StructField("Libellé", StringType(), True),
                StructField("PIB_en_euros_par_habitant", IntegerType(), True),
            ]
        )
        return (
            self.spark.read.option("header", "true")
            .option("delimiter", ";")
            .schema(schema)
            .csv(csv_path)
        )

    def _select_and_rename_pib_2022_columns(self, df):
        """Sélectionne et renomme les colonnes pour les données PIB 2022."""
        return df.select(
            col("Code_INSEE_Région"),
            lit(2022).alias("Année"),
            col("PIB_en_euros_par_habitant"),
        )

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
            return self._extracted_from_extract_inflation_data_23(excel_path, schema)
        except Exception as e:
            logger.error(f"❌ Erreur extraction Excel inflation : {str(e)}")
            return None

    def _extracted_from_extract_inflation_data_23(self, excel_path, schema):
        df = self._load_inflation_data(excel_path, schema)
        df = self._rename_inflation_column(df)
        logger.info(f"🛠️ Colonnes après extraction et renommage : {df.columns}")
        return self._filter_inflation_years(df)

    def _load_inflation_data(self, excel_path, schema):
        """Charge les données d'inflation depuis Excel avec le schéma spécifié."""
        return (
            self.spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("sheetName", "Question 1")
            .option("dataAddress", "'Question 1'!A4")
            .schema(schema)
            .load(excel_path)
        )

    def _rename_inflation_column(self, df):
        """Renomme la colonne d'inflation pour supprimer les espaces."""
        return df.withColumnRenamed(
            "Évolution des prix à la consommation",
            "Évolution_des_prix_à_la_consommation",
        )

    def _filter_inflation_years(self, df):
        """Filtre les données d'inflation pour les années entre 2000 et 2022."""
        return df.filter((col("Année") >= 2000) & (col("Année") <= 2022))

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
        logger.info(
            f"📥 Extraction des données électorales 1965-2012 depuis : {file_pattern}"
        )
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
                logger.error(
                    f"❌ Erreur lors de l'extraction du fichier {file_path}: {str(e)}"
                )
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

    def extract_life_expectancy_data(self, file_path):
        """
        Charge le fichier CSV 'valeurs_anuelles.csv' contenant les données d'espérance de vie à la naissance.
        Le fichier est structuré avec une ligne d'en-tête contenant :
        "Libellé";"idBank";"Dernière mise à jour";"Période";"1901";"1902"; ... ;"2024"

        :param file_path: Chemin du fichier CSV à charger
        :return: DataFrame PySpark contenant les données brutes d'espérance de vie
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ Fichier non trouvé : {file_path}")
            return None

        logger.info(
            f"📥 Extraction des données d'espérance de vie depuis : {file_path}"
        )

        try:
            return (
                self.spark.read.option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(file_path)
            )
        except Exception as e:
            logger.error(
                f"❌ Erreur lors de l'extraction des données d'espérance de vie : {str(e)}"
            )
            return None

    def extract_departments_data(self, file_path):
        """
        Extrait les données des départements depuis le fichier CSV "departements-france.csv".
        Le fichier contient les colonnes : code_departement, nom_departement, code_region, nom_region.

        :param file_path: Chemin du fichier CSV des départements.
        :return: DataFrame PySpark avec les données des départements.
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ Fichier de départements non trouvé : {file_path}")
            return None

        logger.info(f"📥 Extraction des données de départements depuis : {file_path}")

        schema = StructType(
            [
                StructField("code_departement", StringType(), True),
                StructField("nom_departement", StringType(), True),
                StructField("code_region", StringType(), True),
                StructField("nom_region", StringType(), True),
            ]
        )

        try:
            return (
                self.spark.read.option("header", "true")
                .option("delimiter", ",")
                .schema(schema)
                .csv(file_path)
            )
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'extraction des départements : {str(e)}")
            return None

    def extract_education_data(self, input_path: str):
        """
        Extrait les données d'éducation depuis un fichier CSV.
        Le fichier est attendu avec un en-tête, un séparateur ';' et un encodage UTF-8.

        :param input_path: Chemin du fichier CSV d'éducation.
        :return: DataFrame PySpark contenant les données d'éducation.
        """
        if not os.path.exists(input_path):
            logger.error(f"❌ Fichier d'éducation non trouvé : {input_path}")
            return None

        logger.info(f"📥 Extraction des données d'éducation depuis : {input_path}")
        try:
            df = (
                self.spark.read.option("header", "true")
                .option("sep", ";")
                .option("encoding", "UTF-8")
                .csv(input_path)
            )
            logger.info(
                f"✓ Données d'éducation chargées avec succès depuis {input_path}"
            )
            logger.info(f"✅ Nombre de lignes: {df.count()}")
            logger.info(f"✅ Colonnes présentes: {', '.join(df.columns)}")
            return df
        except Exception as e:
            logger.error(
                f"❌ Erreur lors du chargement du fichier d'éducation : {str(e)}"
            )
            return None

    def extract_security_data(self, excel_path):
        """
        Extrait les données de sécurité directement avec pandas sans utiliser Spark.
        Lit chaque feuille (département) du fichier Excel des tableaux 4001.

        :param excel_path: Chemin du fichier Excel contenant les données de sécurité
        :return: DataFrame pandas avec les données brutes
        """
        if not os.path.exists(excel_path):
            logger.error(f"❌ Fichier non trouvé : {excel_path}")
            return None

        logger.info(f"📥 Extraction des données de sécurité depuis : {excel_path}")

        try:
            import pandas as pd

            # Lire les noms des feuilles
            xls = pd.ExcelFile(excel_path)
            dept_sheets = [sheet for sheet in xls.sheet_names if sheet.isdigit()]

            logger.info(
                f"✓ {len(dept_sheets)} départements identifiés dans le fichier Excel"
            )

            # Liste pour stocker les DataFrames pandas
            all_dfs = []

            # Lire chaque feuille avec pandas
            for dept in dept_sheets:
                logger.info(f"🤖 Lecture de la feuille du département {dept}")
                try:
                    df_sheet = pd.read_excel(excel_path, sheet_name=dept)
                    df_sheet["departement"] = dept
                    all_dfs.append(df_sheet)
                except Exception as e:
                    logger.warning(f"⚠️ Problème avec la feuille {dept}: {str(e)}")
                    continue

            # Combiner tous les DataFrames pandas
            if not all_dfs:
                logger.error("❌ Aucune donnée trouvée dans le fichier Excel")
                return None

            df_combined = pd.concat(all_dfs, ignore_index=True)
            logger.info(
                f"✅ Extraction réussie: {len(df_combined)} lignes, {len(df_combined.columns)} colonnes"
            )

            # Information sur les colonnes d'années
            year_cols = [
                col
                for col in df_combined.columns
                if isinstance(col, str) and col.startswith("_")
            ]
            logger.info(f"✅ {len(year_cols)} colonnes d'années identifiées")

            return df_combined

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'extraction: {str(e)}")
            import traceback

            logger.error(traceback.format_exc())
            return None

    def extract_demography_data(self, file_xls, file_xlsx, file_csv):
        """
        Convertit le fichier XLS de démographie en XLSX, fusionne les feuilles en un CSV,
        puis charge les données dans un DataFrame Spark.
        Cette méthode reprend le code de conversion et d'extraction présent dans transform_demoV2.
        """
        # --- Conversion XLS -> XLSX ---
        if not os.path.exists(file_xlsx):
            print("📥 Conversion du fichier XLS en XLSX...")
            try:
                excel_data = pd.read_excel(file_xls, sheet_name=None)
                with pd.ExcelWriter(file_xlsx, engine="openpyxl") as writer:
                    for sheet, data in excel_data.items():
                        data.to_excel(writer, sheet_name=sheet, index=False)
                print(f"✅ Conversion réussie : {file_xlsx}")
            except Exception as e:
                print(f"❌ Erreur lors de la conversion : {e}")
        else:
            print(f"✓ {file_xlsx} existe déjà. Conversion ignorée.")

        # --- Extraction et fusion des feuilles en CSV ---
        if not os.path.exists(file_csv):
            print("📥 Extraction et fusion des feuilles Excel...")
            try:
                df_final = self._extract_and_combine_demo_sheets(file_xlsx)
                # Sauvegarder en CSV avec le séparateur point-virgule
                df_final.to_csv(file_csv, index=False, sep=";")
                print(f"✅ Fichier CSV généré : {file_csv}")
            except Exception as e:
                print(f"❌ Erreur lors de l'extraction/fusion : {e}")
        else:
            print(f"✓ {file_csv} existe déjà. Extraction ignorée.")

        # --- Chargement du CSV avec Spark ---
        return self.spark.read.option("header", True).option("sep", ";").csv(file_csv)

    def extract_orientation_politique(self, file_path):
        """
        Extrait les données d'orientation politique depuis un fichier CSV.
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ Fichier non trouvé : {file_path}")
            return None
            
        logger.info(f"📥 Extraction des données d'orientation politique depuis : {file_path}")
        return self.spark.read.option("header", True).csv(file_path)

    def stop(self):
        """
        Arrête la session Spark si elle est active.
        """
        if self.spark:
            self.spark.stop()
            logger.info("🤖✅ Session Spark arrêtée proprement.")
