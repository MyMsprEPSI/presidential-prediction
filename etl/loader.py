import logging
import os
import shutil
import pandas as pd
from typing import Optional
import mysql.connector
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Classe permettant d'enregistrer un DataFrame PySpark transformé en fichier CSV
    ou de les charger dans une base de données MySQL.
    """

    def __init__(self, spark, output_dir="data\processed_data", 
                 jdbc_url=None, user=None, password=None, database=None, 
                 driver="com.mysql.cj.jdbc.Driver"):
        """
        Initialise le DataLoader avec un répertoire de sortie et des paramètres de connexion à MySQL.

        :param spark: Session Spark
        :param output_dir: Dossier où seront stockés les fichiers CSV transformés
        :param jdbc_url: URL JDBC pour la connexion MySQL
        :param user: Nom d'utilisateur MySQL
        :param password: Mot de passe MySQL
        :param database: Base de données MySQL
        :param driver: Driver JDBC à utiliser
        """
        logger.info(
            f"🚀 Initialisation du DataLoader avec le dossier de sortie : {output_dir}"
        )
        self.spark = spark
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas
        
        # Paramètres de connexion MySQL
        self.jdbc_url = jdbc_url or os.getenv("MYSQL_JDBC_URL", "jdbc:mysql://localhost:3306")
        self.user = user or os.getenv("MYSQL_USER", "root")
        self.password = password or os.getenv("MYSQL_PASSWORD", "")
        self.database = database or os.getenv("MYSQL_DATABASE", "elections_presidentielles")
        self.driver = driver
        
        logger.info("✅ Dossier de sortie créé/validé")
        logger.info(f"✅ Configuration MySQL: {self.jdbc_url}/{self.database}")

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
    
    # Méthodes de chargement des données vers MySQL
    def load_dim_politique(self, df_dim_politique, mode="append"):
        """Charge les données de la dimension politique dans MySQL"""
        self._load(df_dim_politique, "dim_politique", mode)
        logger.info("✨ Chargement des données dans la table dim_politique terminé")
    
    def load_dim_securite(self, df_dim_securite, mode="append"):
        """Charge les données de la dimension sécurité dans MySQL"""
        self._load(df_dim_securite, "dim_securite", mode)
        logger.info("✨ Chargement des données dans la table dim_securite terminé")
    
    def load_dim_sante(self, df_dim_sante, mode="append"):
        """Charge les données de la dimension santé dans MySQL"""
        self._load(df_dim_sante, "dim_sante", mode)
        logger.info("✨ Chargement des données dans la table dim_sante terminé")
    
    def load_dim_education(self, df_dim_education, mode="append"):
        """Charge les données de la dimension éducation dans MySQL"""
        self._load(df_dim_education, "dim_education", mode)
        logger.info("✨ Chargement des données dans la table dim_education terminé")
    
    def load_dim_environnement(self, df_dim_environnement, mode="append"):
        """Charge les données de la dimension environnement dans MySQL"""
        self._load(df_dim_environnement, "dim_environnement", mode)
        logger.info("✨ Chargement des données dans la table dim_environnement terminé")
    
    def load_dim_socio_economie(self, df_dim_socio_economie, mode="append"):
        """Charge les données de la dimension socio-économie dans MySQL"""
        self._load(df_dim_socio_economie, "dim_socio_economie", mode)
        logger.info("✨ Chargement des données dans la table dim_socio_economie terminé")
    
    def load_dim_technologie(self, df_dim_technologie, mode="append"):
        """Charge les données de la dimension technologie dans MySQL"""
        self._load(df_dim_technologie, "dim_technologie", mode)
        logger.info("✨ Chargement des données dans la table dim_technologie terminé")
    
    def load_dim_demographie(self, df_dim_demographie, mode="append"):
        """Charge les données de la dimension démographie dans MySQL"""
        self._load(df_dim_demographie, "dim_demographie", mode)
        logger.info("✨ Chargement des données dans la table dim_demographie terminé")
    
    def load_fact_resultats_politique(self, df_fact_resultats_politique, mode="append"):
        """Charge les données de la table de faits résultats_politique dans MySQL"""
        self._load(df_fact_resultats_politique, "fact_resultats_politique", mode)
        logger.info("✨ Chargement des données dans la table fact_resultats_politique terminé")

    def _load(self, df, table_name, mode):
        """
        Réalise l'écriture JDBC dans MySQL.
        :param df: DataFrame à charger
        :param table_name: Nom de la table cible
        :param mode: Mode d'insertion ('append', 'overwrite', etc.)
        """
        props = {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }
        
        # Désactivation temporaire des contraintes FK
        try:
            conn = mysql.connector.connect(
                host=self._get_host_from_jdbc(),
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=0;", conn, "Foreign key checks disabled.")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la désactivation des contraintes FK: {str(e)}")
            conn = None

        try:
            # Construire l'URL complète avec la base de données
            full_jdbc_url = f"{self.jdbc_url}/{self.database}"
            
            # Si le mode est 'overwrite', utiliser TRUNCATE au lieu de DROP
            if mode == "overwrite" and conn is not None:
                try:
                    # Tronquer la table au lieu de la supprimer
                    self._execute_sql(cursor, f"TRUNCATE TABLE {table_name};", conn, f"Table {table_name} truncated.")
                    # Passer en mode append pour éviter que Spark ne tente de DROP la table
                    mode = "append"
                except Exception as ex:
                    logger.warning(f"Impossible de tronquer la table {table_name}: {ex}")
            
            # Écriture des données
            df.write.jdbc(
                url=full_jdbc_url,
                table=table_name,
                mode=mode,
                properties=props
            )
            logger.info(f"⚡ Données chargées dans la table {table_name} avec succès.")
            
            # Réactiver les contraintes FK
            if conn:
                self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=1;", conn, "Foreign key checks enabled.")
                conn.close()
        except Exception as e:
            logger.error(f"⛔ Erreur lors du chargement dans la table {table_name}: {str(e)}")
            if conn:
                try:
                    self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=1;", conn, "Foreign key checks re-enabled.")
                    conn.close()
                except:
                    pass


    def _execute_sql(self, cursor, sql_statement, conn, success_message):
        """
        Exécute une instruction SQL et valide la transaction.
        
        :param cursor: Curseur MySQL
        :param sql_statement: Instruction SQL à exécuter
        :param conn: Connexion MySQL
        :param success_message: Message de succès à afficher
        """
        cursor.execute(sql_statement)
        conn.commit()
        logger.info(success_message)

    def _get_host_from_jdbc(self):
        """
        Extrait le nom d'hôte de l'URL JDBC.
        
        :return: Nom d'hôte
        """
        try:
            url_without_prefix = self.jdbc_url.split("://")[1]
            host_port = url_without_prefix.split("/")[0]
            return host_port.split(":")[0]
        except Exception:
            return "localhost"
    
    def initialize_database(self):
        """
        Initialise la base de données en créant les tables si elles n'existent pas.
        """
        try:
            conn = mysql.connector.connect(
                host=self._get_host_from_jdbc(),
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Création de la base de données si elle n'existe pas
            self._execute_sql(
                cursor,
                f"CREATE DATABASE IF NOT EXISTS {self.database};",
                conn,
                f"✅ Base de données '{self.database}' créée ou déjà existante."
            )
            
            # Utiliser la base de données
            self._execute_sql(cursor, f"USE {self.database};", conn, f"✅ Utilisation de la base '{self.database}'.")
            
            # Création des tables selon le schéma
            # Table dim_politique
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_politique` (
              `id` INT PRIMARY KEY,
              `etiquette_parti` INT,
              `annee` INT,
              `code_dept` VARCHAR(3),
              `candidat` VARCHAR(100),
              `total_voix` INT,
              `orientation_politique` VARCHAR(50)
            );
            """, conn, "✅ Table dim_politique créée ou déjà existante.")
            
            # Table dim_securite
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_securite` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `annee` INT,
              `code_dept` VARCHAR(3),
              `delits_total` INT
            );
            """, conn, "✅ Table dim_securite créée ou déjà existante.")
            
            # Table dim_sante
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_sante` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `code_dept` VARCHAR(3),
              `annee` INT,
              `esperance_vie` FLOAT
            );
            """, conn, "✅ Table dim_sante créée ou déjà existante.")
            
            # Table dim_education
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_education` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `code_departement` VARCHAR(3),
              `annee_fermeture` INT,
              `libelle_departement` VARCHAR(100),
              `nombre_total_etablissements` INT,
              `nb_public` INT,
              `nb_prive` INT,
              `pct_public` FLOAT,
              `pct_prive` FLOAT
            );
            """, conn, "✅ Table dim_education créée ou déjà existante.")
            
            # Table dim_environnement
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_environnement` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `code_insee_region` VARCHAR(3),
              `annee` INT,
              `parc_eolien_mw` FLOAT,
              `parc_solaire_mw` FLOAT
            );
            """, conn, "✅ Table dim_environnement créée ou déjà existante.")
            
            # Table dim_socio_economie
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_socio_economie` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `annee` INT,
              `pib_euros_par_habitant` FLOAT,
              `code_insee_region` VARCHAR(3),
              `evolution_prix_conso` FLOAT,
              `pib_par_inflation` FLOAT
            );
            """, conn, "✅ Table dim_socio_economie créée ou déjà existante.")
            
            # Table dim_technologie
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_technologie` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `annee` INT,
              `depenses_rd_pib` FLOAT
            );
            """, conn, "✅ Table dim_technologie créée ou déjà existante.")
            
            # Table dim_demographie
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `dim_demographie` (
              `id` INT PRIMARY KEY AUTO_INCREMENT,
              `annee` INT,
              `code_departement` VARCHAR(3),
              `nom_departement` VARCHAR(100),
              `population_totale` INT,
              `population_hommes` INT,
              `population_femmes` INT,
              `pop_0_19` INT,
              `pop_20_39` INT,
              `pop_40_59` INT,
              `pop_60_74` INT,
              `pop_75_plus` INT
            );
            """, conn, "✅ Table dim_demographie créée ou déjà existante.")
            
            # Table fact_resultats_politique
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `fact_resultats_politique` (
              `annee_code_dpt` VARCHAR(10) PRIMARY KEY,
              `id_parti` INT,
              `securite_id` INT,
              `socio_eco_id` INT,
              `sante_id` INT,
              `environnement_id` INT,
              `education_id` INT,
              `demographie_id` INT,
              `technologie_id` INT
            );
            """, conn, "✅ Table fact_resultats_politique créée ou déjà existante.")
            
            # Ajout des contraintes de clés étrangères
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_politique` 
            FOREIGN KEY (`id_parti`) REFERENCES `dim_politique` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_politique")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_securite` 
            FOREIGN KEY (`securite_id`) REFERENCES `dim_securite` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_securite")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_socio_eco` 
            FOREIGN KEY (`socio_eco_id`) REFERENCES `dim_socio_economie` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_socio_economie")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_sante` 
            FOREIGN KEY (`sante_id`) REFERENCES `dim_sante` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_sante")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_environnement` 
            FOREIGN KEY (`environnement_id`) REFERENCES `dim_environnement` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_environnement")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_education` 
            FOREIGN KEY (`education_id`) REFERENCES `dim_education` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_education")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_demographie` 
            FOREIGN KEY (`demographie_id`) REFERENCES `dim_demographie` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_demographie")
            
            self._execute_sql(cursor, """
            ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_technologie` 
            FOREIGN KEY (`technologie_id`) REFERENCES `dim_technologie` (`id`) ON DELETE SET NULL;
            """, conn, "✅ Contrainte FK ajoutée: fact_resultats_politique -> dim_technologie")
            
            conn.close()
            logger.info("✅ Initialisation de la base de données terminée avec succès!")
            
        except mysql.connector.Error as err:
            logger.error(f"❌ Erreur MySQL lors de l'initialisation de la base: {err}")
        except Exception as e:
            logger.error(f"❌ Erreur générale lors de l'initialisation de la base: {str(e)}")

    def drop_fact_table(self):
        """
        Supprime la table de faits pour libérer les contraintes avant le rechargement des dimensions
        """
        try:
            conn = mysql.connector.connect(
                host=self._get_host_from_jdbc(),
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()

            # Désactiver les vérifications de FK
            self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=0;", conn, "Foreign key checks disabled.")
            
            # Supprimer la table de faits
            self._execute_sql(cursor, "DROP TABLE IF EXISTS fact_resultats_politique;", conn, 
                            "Table fact_resultats_politique supprimée.")
            
            # Recréer la structure de la table
            self._execute_sql(cursor, """
            CREATE TABLE IF NOT EXISTS `fact_resultats_politique` (
            `annee_code_dpt` VARCHAR(10) PRIMARY KEY,
            `id_parti` INT,
            `securite_id` INT,
            `socio_eco_id` INT,
            `sante_id` INT,
            `environnement_id` INT,
            `education_id` INT,
            `demographie_id` INT,
            `technologie_id` INT
            );
            """, conn, "Table fact_resultats_politique recréée.")
            
            # Réactiver les vérifications de FK
            self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=1;", conn, "Foreign key checks enabled.")
            
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Erreur lors de la suppression de la table de faits: {str(e)}")
            return False

    def truncate_and_load_dim(self, table_name, df):
        """
        Vide une table avec TRUNCATE puis y charge des données
        
        Args:
            table_name: Nom de la table à vider
            df: DataFrame à charger
        """
        try:
            if df is None:
                logger.warning(f"⚠️ DataFrame pour {table_name} est None, chargement ignoré")
                return False
                
            conn = mysql.connector.connect(
                host=self._get_host_from_jdbc(),
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            
            # Désactiver FK checks
            self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=0;", conn, "Foreign key checks disabled.")
            
            # Vider la table avec TRUNCATE
            self._execute_sql(cursor, f"TRUNCATE TABLE {table_name};", conn, f"Table {table_name} vidée.")
            
            # Construire l'URL complète avec la base de données
            full_jdbc_url = f"{self.jdbc_url}/{self.database}"
            
            # Propriétés de connexion
            props = {
                "user": self.user,
                "password": self.password,
                "driver": self.driver
            }
            
            # Charger les données en mode append (la table existe déjà)
            df.write.jdbc(
                url=full_jdbc_url,
                table=table_name,
                mode="append",
                properties=props
            )
            
            # Réactiver les FK checks
            self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=1;", conn, "Foreign key checks enabled.")
            
            cursor.close()
            conn.close()
            logger.info(f"✅ Données chargées dans {table_name} avec succès")
            return True
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement dans {table_name}: {str(e)}")
            return False
    
                
    def generate_consolidated_csv_from_files(
        self,
        election_csv,    # Chemin vers le CSV des données politiques
        security_csv,    # Chemin vers le CSV de la sécurité
        socio_csv,       # Chemin vers le CSV de la socio-économie
        sante_csv,       # Chemin vers le CSV de la santé
        env_csv,         # Chemin vers le CSV de l'environnement
        edu_csv,         # Chemin vers le CSV de l'éducation
        demo_csv,        # Chemin vers le CSV de la démographie
        tech_csv,        # Chemin vers le CSV de la technologie
        output_filename="consolidated_data.csv"
    ):
        from pyspark.sql.functions import col, lit, concat, lpad, coalesce, first, trim
        from pyspark.sql.types import StringType

        logger.info("🚀 Génération du fichier consolidé à partir des CSV...")

        # Variables constantes pour la lisibilité
        DEPT_FILE = "data/politique/departements-france.csv"
        TARGET_YEARS = [2002, 2007, 2012, 2017, 2022]
        
        # Liste des départements désirés (métropole, sans Corse)
        # Utilisation d'une compréhension de liste plus claire
        desired_depts = [f"{i:02d}" for i in range(1, 96) if i != 20]

        # Fonction d'aide pour lire les CSV avec des options standard
        def read_csv_with_options(path, delimiter=";"):
            return self.spark.read.option("header", "true").option("delimiter", delimiter).csv(path)

        # Fichier mapping départements-régions
        df_depts = self.spark.read.option("header", "true").csv(DEPT_FILE) \
            .select(
                trim(col("code_region")).alias("region"),
                col("code_departement").alias("dept")
            ).filter(col("dept").isin(desired_depts))

        # Lecture et transformation des fichiers départementaux
        # Utilisation d'une structure plus cohérente pour les sélections
        df_pol = read_csv_with_options(election_csv) \
            .select(
                col("annee").cast("int"),
                lpad(col("code_dept"), 2, "0").alias("dept"),
                col("id_parti").alias("politique (parti)")
            )

        df_sec = read_csv_with_options(security_csv) \
            .select(
                col("Année").cast("int").alias("annee"),
                lpad(col("Département"), 2, "0").alias("dept"),
                col("Délits_total").alias("securite (Nombre_de_délits)")
            )

        df_sat = read_csv_with_options(sante_csv) \
            .select(
                col("Année").cast("int").alias("annee"),
                lpad(col("CODE_DEP"), 2, "0").alias("dept"),
                col("Espérance_Vie").alias("sante (Espérance_de_Vie_H/F)")
            )

        # Pour l'éducation, optimisation de la conversion et du format
        df_ed = read_csv_with_options(edu_csv) \
            .select(
                col("annee_fermeture").cast("int").alias("annee"),
                lpad(col("code_departement").cast("int").cast("string"), 2, "0").alias("dept"),
                col("nombre_total_etablissements").cast("int").alias("education (Nombre_Total_Établissements)")
            ) \
            .filter(col("annee").isin(TARGET_YEARS)) \
            .groupBy("annee", "dept") \
            .agg(first("education (Nombre_Total_Établissements)").alias("education (Nombre_Total_Établissements)"))

        df_dem = read_csv_with_options(demo_csv) \
            .select(
                col("Année").cast("int").alias("annee"),
                lpad(col("Code_Département"), 2, "0").alias("dept"),
                col("E_Total").alias("demographie (Population_Totale)")
            )

        df_tech = read_csv_with_options(tech_csv) \
            .select(
                col("annee").cast("int"),
                col("dird_pib_france_pourcentages").alias("technologie (Dépenses_en_R&D_en_pourcentages)")
            )

        # Factorisation du code pour les fichiers régionaux
        def process_regional_file(csv_path, value_col_name, output_col_name):
            return read_csv_with_options(csv_path) \
                .select(
                    coalesce(col("Année"), col("année")).cast("int").alias("annee"),
                    trim(col("Code_INSEE_Région")).alias("region"),
                    col(value_col_name).alias(output_col_name)
                ) \
                .groupBy("annee", "region") \
                .agg(first(output_col_name).alias(output_col_name)) \
                .join(df_depts, on="region", how="inner") \
                .drop("region")

        # Application de la fonction factoriséE
        df_soc = process_regional_file(socio_csv, "PIB_par_inflation", "socio_economie (PIB_par_Inflation)")
        df_envr = process_regional_file(env_csv, "Parc_installé_éolien_MW", "environnemental (Parc_installé_éolien_MW)")

        # Jointure progressive et lisible avec des variables intermédiaires pour faciliter le débogage
        df_base = df_pol.join(df_sec, ["annee", "dept"], "full_outer")
        df_mid = df_base.join(df_soc, ["annee", "dept"], "full_outer") \
                        .join(df_sat, ["annee", "dept"], "full_outer") \
                        .join(df_envr, ["annee", "dept"], "full_outer")
        df_join = df_mid.join(df_ed, ["annee", "dept"], "left") \
                        .join(df_dem, ["annee", "dept"], "full_outer") \
                        .join(df_tech, ["annee"], "left")

        # Filtrage avec une condition lisible
        df_filtered = df_join.filter(
            (col("annee").isin(TARGET_YEARS)) & (col("dept").isin(desired_depts))
        )

        # Ajout d'une colonne clé pour identification
        df_with_key = df_filtered.withColumn(
            "annee_code_dpt", 
            concat(col("annee").cast("string"), lit("_"), col("dept"))
        )

        # Colonnes finales pour le rapport
        output_columns = [
            "annee_code_dpt",
            "politique (parti)",
            "securite (Nombre_de_délits)",
            "socio_economie (PIB_par_Inflation)",
            "sante (Espérance_de_Vie_H/F)",
            "environnemental (Parc_installé_éolien_MW)",
            "education (Nombre_Total_Établissements)",
            "demographie (Population_Totale)",
            "technologie (Dépenses_en_R&D_en_pourcentages)"
        ]
        
        # Sélection et ordonnancement
        df_final = df_with_key.select(*output_columns).orderBy("annee_code_dpt")

        logger.info("✅ Données consolidées prêtes. Aperçu :")
        df_final.show(10, truncate=False)

        self.save_to_csv(df_final, output_filename)