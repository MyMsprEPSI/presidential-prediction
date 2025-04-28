# loader.py

import logging
import os
import shutil
import pandas as pd
import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, List, Any, Union

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Classe permettant d'enregistrer un DataFrame PySpark transformé en fichier CSV et en base de données MySQL.
    """

    def __init__(self, spark, output_dir="data\processed_data", db_config=None):
        """
        Initialise le DataLoader avec un répertoire de sortie et une configuration de base de données optionnelle.

        :param spark: Session Spark
        :param output_dir: Dossier où seront stockés les fichiers CSV transformés.
        :param db_config: Dictionnaire de configuration de la base de données MySQL
        """
        logger.info(
            f"🚀 Initialisation du DataLoader avec le dossier de sortie : {output_dir}"
        )
        self.spark = spark
        self.output_dir = output_dir
        self.db_config = db_config or {
            'host': 'localhost',
            'user': 'root',
            'password': '',
            'database': 'elections_presidentielles',
            'port': 3306
        }
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
                    logger.error("❌ Aucun fichier CSV généré dans le dossier temporaire.")
            else:  # C'est un DataFrame pandas
                # Pour le fichier de sécurité, on utilise toujours le header
                df.to_csv(final_output_path, sep=";", index=False, header=True)
                logger.info("✅ Fichier CSV sauvegardé avec succès via pandas!")

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement du fichier : {str(e)}")
            if os.path.exists(final_output_path + "_temp"):
                shutil.rmtree(final_output_path + "_temp")

    def _establish_connection(self):
        """Établit une connexion à la base de données MySQL."""
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                logger.info("✅ Connexion à MySQL établie")
                return connection
        except Error as e:
            logger.error(f"❌ Erreur lors de la connexion à MySQL: {e}")
            return None

    def setup_database(self):
        """
        Configure la base de données avec le schéma en étoile.
        Crée la base de données et les tables si elles n'existent pas.
        """
        connection = None
        try:
            # Configuration temporaire sans spécifier la base de données
            temp_config = self.db_config.copy()
            if 'database' in temp_config:
                temp_config.pop('database')
            
            connection = mysql.connector.connect(**temp_config)
            cursor = connection.cursor()
            
            # Création de la base de données si elle n'existe pas
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']}")
            cursor.execute(f"USE {self.db_config['database']}")
            
            # Création des tables dimension et fait
            tables_sql = [
                """
                CREATE TABLE IF NOT EXISTS `dim_politique` (
                  `id` INT AUTO_INCREMENT PRIMARY KEY,
                  `etiquette_parti` VARCHAR(50),
                  `annee` INT,
                  `code_dept` VARCHAR(3),
                  `candidat` VARCHAR(100),
                  `total_voix` INT,
                  `orientation_politique` VARCHAR(50)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS `dim_securite` (
                  `id` INT PRIMARY KEY AUTO_INCREMENT,
                  `annee` INT,
                  `code_dept` VARCHAR(4),
                  `delits_total` INT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS `dim_sante` (
                  `id` INT PRIMARY KEY AUTO_INCREMENT,
                  `code_dept` VARCHAR(3),
                  `annee` INT,
                  `esperance_vie` FLOAT
                );
                """,
                """
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
                """,
                """
                CREATE TABLE IF NOT EXISTS `dim_environnement` (
                  `id` INT PRIMARY KEY AUTO_INCREMENT,
                  `code_insee_region` VARCHAR(3),
                  `annee` INT,
                  `parc_eolien_mw` FLOAT,
                  `parc_solaire_mw` FLOAT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS `dim_socio_economie` (
                  `id` INT PRIMARY KEY AUTO_INCREMENT,
                  `annee` INT,
                  `pib_euros_par_habitant` FLOAT,
                  `code_insee_region` VARCHAR(3),
                  `evolution_prix_conso` FLOAT,
                  `pib_par_inflation` FLOAT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS `dim_technologie` (
                  `id` INT PRIMARY KEY AUTO_INCREMENT,
                  `annee` INT,
                  `depenses_rd_pib` FLOAT
                );
                """,
                """
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
                """,
                """
                CREATE TABLE IF NOT EXISTS `fact_resultats_politique` (
                  `id` INT AUTO_INCREMENT,
                  `annee_code_dpt` VARCHAR(10),
                  `id_parti` INT,
                  `securite_id` INT,
                  `socio_eco_id` INT,
                  `sante_id` INT,
                  `environnement_id` INT,
                  `education_id` INT,
                  `demographie_id` INT,
                  `technologie_id` INT,
                  PRIMARY KEY (`id`),
                  UNIQUE KEY (`annee_code_dpt`)
                );
                """,
                # Contraintes
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_parti` 
                FOREIGN KEY (`id_parti`) REFERENCES `dim_politique` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_securite` 
                FOREIGN KEY (`securite_id`) REFERENCES `dim_securite` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_socio_eco` 
                FOREIGN KEY (`socio_eco_id`) REFERENCES `dim_socio_economie` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_sante` 
                FOREIGN KEY (`sante_id`) REFERENCES `dim_sante` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_environnement` 
                FOREIGN KEY (`environnement_id`) REFERENCES `dim_environnement` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_education` 
                FOREIGN KEY (`education_id`) REFERENCES `dim_education` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_demographie` 
                FOREIGN KEY (`demographie_id`) REFERENCES `dim_demographie` (`id`)
                ON DELETE SET NULL;
                """,
                """
                ALTER TABLE `fact_resultats_politique` ADD CONSTRAINT `fk_technologie` 
                FOREIGN KEY (`technologie_id`) REFERENCES `dim_technologie` (`id`)
                ON DELETE SET NULL;
                """
            ]
            
            for sql in tables_sql:
                try:
                    cursor.execute(sql)
                    logger.info("✅ Exécution SQL réussie")
                except Error as e:
                    # Ignorer les erreurs liées aux contraintes déjà existantes
                    if "already exists" not in str(e) and "Duplicate" not in str(e):
                        logger.warning(f"⚠️ Avertissement SQL: {e}")
            
            connection.commit()
            logger.info("✅ Schéma de base de données configuré avec succès")
            
        except Error as e:
            logger.error(f"❌ Erreur lors de la configuration de la base de données: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    def _df_to_pandas(self, df):
        """Convertit un DataFrame Spark en pandas si nécessaire."""
        if hasattr(df, "toPandas"):  # C'est un DataFrame Spark
            return df.toPandas()
        return df  # C'est déjà un DataFrame pandas

    def insert_data_to_mysql(self, table_name: str, df, columns_mapping: Dict[str, str] = None):
        """
        Insère les données d'un DataFrame dans une table MySQL.
        
        :param table_name: Nom de la table MySQL
        :param df: DataFrame à insérer (Spark ou pandas)
        :param columns_mapping: Dictionnaire de mappage {colonne_df: colonne_mysql}
        :return: True si réussi, False sinon
        """
        if df is None:
            logger.error(f"❌ DataFrame vide pour {table_name}")
            return False

        # Conversion en pandas si c'est un DataFrame Spark
        df_pandas = self._df_to_pandas(df)
        
        # Vérification si le DataFrame est vide
        if df_pandas.empty:
            logger.warning(f"⚠️ Aucune donnée à insérer dans {table_name}")
            return True
        
        # Si un mappage des colonnes est fourni, renommer les colonnes
        if columns_mapping:
            df_pandas = df_pandas.rename(columns=columns_mapping)
        
        connection = self._establish_connection()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            
            # Récupérer les noms des colonnes de la table
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            table_columns = [column[0] for column in cursor.fetchall()]
            
            # Filtrer les colonnes du DataFrame qui existent dans la table
            existing_cols = [col for col in df_pandas.columns if col.lower() in [tc.lower() for tc in table_columns]]
            
            if not existing_cols:
                logger.warning(f"⚠️ Aucune colonne du DataFrame ne correspond aux colonnes de la table {table_name}")
                logger.info(f"Colonnes du DataFrame: {list(df_pandas.columns)}")
                logger.info(f"Colonnes de la table: {table_columns}")
                return False
            
            # Créer un nouveau DataFrame avec uniquement les colonnes existantes et les bons noms de colonnes
            mapped_df = pd.DataFrame()
            for col in existing_cols:
                # Trouver le nom exact de la colonne dans table_columns (respecter la casse)
                table_col = next((tc for tc in table_columns if tc.lower() == col.lower()), col)
                mapped_df[table_col] = df_pandas[col]
            
            # Exclure la colonne 'id' si elle est auto-incrémentée
            if 'id' in mapped_df.columns:
                mapped_df = mapped_df.drop(columns=['id'])
            
            # S'il ne reste plus de colonnes, sortir
            if mapped_df.empty or len(mapped_df.columns) == 0:
                logger.warning(f"⚠️ Pas de colonnes valides pour l'insertion dans {table_name}")
                return True
                
            # Préparer les données et la requête SQL
            records = mapped_df.to_dict('records')
            
            if not records:
                logger.warning(f"⚠️ Aucune donnée à insérer dans {table_name}")
                return True
            
            # Obtenir les noms de colonnes après le mappage
            df_columns = list(mapped_df.columns)
            
            # Construire la requête d'insertion
            columns_str = ", ".join([f"`{col}`" for col in df_columns])
            placeholders = ", ".join(["%s"] * len(df_columns))
            
            sql = f"""INSERT INTO {table_name} ({columns_str}) 
                    VALUES ({placeholders})"""
                    
            # Utiliser ON DUPLICATE KEY UPDATE seulement s'il y a une clé primaire autre que 'id'
            if any(col != 'id' for col in table_columns if 'PRI' in col):
                sql += " ON DUPLICATE KEY UPDATE " + ", ".join([f"`{col}` = VALUES(`{col}`)" for col in df_columns])
            
            # Insérer les données par lots pour éviter les problèmes de mémoire
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                values = [[record.get(col) for col in df_columns] for record in batch]
                cursor.executemany(sql, values)
                connection.commit()
                logger.info(f"✅ Lot inséré dans {table_name}: {i+1} à {min(i+batch_size, len(records))}")
            
            logger.info(f"✅ Données insérées avec succès dans la table {table_name}")
            return True
            
        except Error as e:
            logger.error(f"❌ Erreur lors de l'insertion des données dans {table_name}: {e}")
            return False
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def load_to_mysql_from_csv(self, csv_paths, table_mappings):
        """
        Charge les données des fichiers CSV dans les tables MySQL.
        
        :param csv_paths: Dictionnaire {nom_dimension: chemin_csv}
        :param table_mappings: Dictionnaire {nom_dimension: {colonnes_csv: colonnes_mysql}}
        :return: True si réussi, False sinon
        """
        results = []
        
        # Configurer la base de données si nécessaire
        self.setup_database()
        
        # Charger chaque fichier dans sa table respective
        for dim_name, csv_path in csv_paths.items():
            if not os.path.exists(csv_path):
                logger.error(f"❌ Fichier non trouvé: {csv_path}")
                results.append(False)
                continue
                
            table_name = f"dim_{dim_name}"
            mapping = table_mappings.get(dim_name, {})
            
            logger.info(f"🔄 Chargement de {csv_path} dans {table_name}")
            
            # Lecture du CSV
            try:
                df = pd.read_csv(csv_path, sep=';')
                # Insérer dans MySQL
                success = self.insert_data_to_mysql(table_name, df, mapping)
                results.append(success)
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du CSV {csv_path}: {e}")
                results.append(False)
        
        return all(results)
    

    def generate_consolidated_database(
        self,
        election_csv,    # Chemin vers le CSV des données politiques
        security_csv,    # Chemin vers le CSV de la sécurité
        socio_csv,       # Chemin vers le CSV de la socio-économie
        sante_csv,       # Chemin vers le CSV de la santé
        env_csv,         # Chemin vers le CSV de l'environnement
        edu_csv,         # Chemin vers le CSV de l'éducation
        demo_csv,        # Chemin vers le CSV de la démographie
        tech_csv         # Chemin vers le CSV de la technologie
    ):
        """
        Génère les tables de dimensions et de faits dans la base de données MySQL.
        Cette fonction est similaire à generate_consolidated_csv_from_files, mais pour MySQL.
        
        :return: True si réussi, False sinon
        """
        import os
        from pyspark.sql.functions import col, lit, concat, lpad, coalesce, first, trim
        from pyspark.sql.types import StringType

        logger.info("🚀 Génération des tables consolidées pour MySQL...")

        try:
            # Configuration de la base de données
            self.setup_database()
            
            # Définition des mappages de colonnes pour chaque dimension
# Définition des mappages de colonnes pour chaque dimension
            mappings = {
                'environnement': {
                    'Code_INSEE_Région': 'code_insee_region',
                    'Année': 'annee',
                    'Parc_installé_éolien_MW': 'parc_eolien_mw',
                    'Parc_installé_solaire_MW': 'parc_solaire_mw'
                },
                'socio_economie': {
                    'Année': 'annee',
                    'PIB_en_euros_par_habitant': 'pib_euros_par_habitant',
                    'Code_INSEE_Région': 'code_insee_region',
                    'Évolution_des_prix_à_la_consommation': 'evolution_prix_conso',
                    'PIB_par_inflation': 'pib_par_inflation'
                },
                'technologie': {
                    'annee': 'annee',
                    'dird_pib_france_pourcentages': 'depenses_rd_pib'
                },
                'politique': {
                    'annee': 'annee',
                    'code_dept': 'code_dept',
                    'id_parti': 'etiquette_parti',
                    'candidat': 'candidat',
                    'total_voix': 'total_voix',
                    'orientation_politique': 'orientation_politique'
                },
                'sante': {
                    'CODE_DEP': 'code_dept',
                    'Année': 'annee',
                    'Espérance_Vie': 'esperance_vie'
                },
                'securite': {
                    'Année': 'annee',
                    'Département': 'code_dept',
                    'Délits_total': 'delits_total'
                },
                'demographie': {
                    'Année': 'annee',
                    'Code_Département': 'code_departement',
                    'Nom_Département': 'nom_departement',
                    'E_Total': 'population_totale',
                    'H_Total': 'population_hommes',
                    'F_Total': 'population_femmes',
                    'E_0_19_ans': 'pop_0_19',
                    'E_20_39_ans': 'pop_20_39',
                    'E_40_59_ans': 'pop_40_59',
                    'E_60_74_ans': 'pop_60_74',
                    'E_75_et_plus': 'pop_75_plus'
                },
                'education': {
                    'annee_fermeture': 'annee_fermeture',
                    'code_departement': 'code_departement',
                    'libelle_departement': 'libelle_departement',
                    'nombre_total_etablissements': 'nombre_total_etablissements',
                    'nb_public': 'nb_public',
                    'nb_prive': 'nb_prive',
                    'pct_public': 'pct_public',
                    'pct_prive': 'pct_prive'
                }
            }
            
            # Variables constantes
            TARGET_YEARS = list(range(2000, 2023))  # All years from 2000 to 2022 inclusive 
            desired_depts = [f"{i:02d}" for i in range(1, 96) if i != 20]
            
            # 1. Charger les données à partir des fichiers CSV
            csv_paths = {
                'environnement': env_csv,
                'socio_economie': socio_csv,
                'technologie': tech_csv,
                'politique': election_csv,
                'sante': sante_csv,
                'securite': security_csv,
                'demographie': demo_csv,
                'education': edu_csv
            }
            
            # Charger chaque fichier CSV dans sa table correspondante
            for dim_name, csv_path in csv_paths.items():
                try:
                    if os.path.exists(csv_path):
                        logger.info(f"🔄 Chargement de {dim_name} depuis {csv_path}")
                        df = pd.read_csv(csv_path, sep=';')
                        
                        # Appliquer le mappage de colonnes
                        if dim_name in mappings:
                            self.insert_data_to_mysql(f"dim_{dim_name}", df, mappings[dim_name])
                        else:
                            self.insert_data_to_mysql(f"dim_{dim_name}", df)
                    else:
                        logger.error(f"❌ Fichier non trouvé: {csv_path}")
                except Exception as e:
                    logger.error(f"❌ Erreur lors du chargement de {dim_name}: {str(e)}")
            
            # 2. Créer la table de faits
            self.create_fact_table(TARGET_YEARS, desired_depts)
            
            logger.info("✅ Génération de la base de données en étoile terminée avec succès")
            return True
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération de la base de données: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def create_fact_table(self, target_years, desired_depts):
        """
        Crée la table de faits à partir des dimensions en utilisant une correspondance
        département-région directement depuis le fichier CSV, sans dépendre d'une table en base de données.
        """
        connection = None
        DEPT_FILE = "data/origine/politique/departements-france.csv"
        
        try:
            # Charger le mapping département-région depuis le fichier CSV
            dept_to_region = {}
            if os.path.exists(DEPT_FILE):
                try:
                    df_dept_mapping = pd.read_csv(DEPT_FILE)
                    # Créer un dictionnaire de correspondance {code_dept: code_region}
                    for _, row in df_dept_mapping.iterrows():
                        dept_code = str(row['code_departement']).zfill(2)  # Format sur 2 chiffres
                        region_code = str(row['code_region']).strip()
                        dept_to_region[dept_code] = region_code
                except Exception as e:
                    logger.warning(f"⚠️ Impossible de charger le fichier de correspondance département-région: {str(e)}")
            
            connection = self._establish_connection()
            if not connection:
                logger.error("❌ Impossible de se connecter à MySQL pour les données de faits")
                return False
                
            cursor = connection.cursor()
            
            # Préparation d'un DataFrame pour la table de faits
            fact_data = []
            
            # Pour chaque combinaison année-département
            for year in target_years:
                for dept in desired_depts:
                    annee_code_dpt = f"{year}_{dept}"
                    
                    # Récupérer les IDs pour chaque dimension
                    
                    # Politique
                    cursor.execute(f"""SELECT id FROM dim_politique 
                                    WHERE annee = {year} AND code_dept = '{dept}'
                                    LIMIT 1""")
                    result = cursor.fetchone()
                    id_parti = result[0] if result else None
                    
                    # Sécurité
                    cursor.execute(f"""SELECT id FROM dim_securite 
                                    WHERE annee = {year} AND code_dept = '{dept}'
                                    LIMIT 1""")
                    result = cursor.fetchone()
                    securite_id = result[0] if result else None
                    
                    # Santé
                    cursor.execute(f"""SELECT id FROM dim_sante 
                                    WHERE annee = {year} AND code_dept = '{dept}'
                                    LIMIT 1""")
                    result = cursor.fetchone()
                    sante_id = result[0] if result else None
                    
                    # Éducation
                    cursor.execute(f"""SELECT id FROM dim_education 
                                    WHERE annee_fermeture = {year} AND code_departement = '{dept}'
                                    LIMIT 1""")
                    result = cursor.fetchone()
                    education_id = result[0] if result else None
                    
                    # Démographie
                    cursor.execute(f"""SELECT id FROM dim_demographie 
                                    WHERE annee = {year} AND code_departement = '{dept}'
                                    LIMIT 1""")
                    result = cursor.fetchone()
                    demographie_id = result[0] if result else None
                    
                    # Récupérer le code région directement du dictionnaire
                    region_code = dept_to_region.get(dept)
                    
                    socio_eco_id = None
                    environnement_id = None
                    
                    if region_code:
                        # Socio-économie
                        cursor.execute(f"""SELECT id FROM dim_socio_economie 
                                        WHERE annee = {year} AND code_insee_region = '{region_code}'
                                        LIMIT 1""")
                        result = cursor.fetchone()
                        socio_eco_id = result[0] if result else None
                        
                        # Environnement
                        cursor.execute(f"""SELECT id FROM dim_environnement 
                                        WHERE annee = {year} AND code_insee_region = '{region_code}'
                                        LIMIT 1""")
                        result = cursor.fetchone()
                        environnement_id = result[0] if result else None
                    
                    # Technologie (niveau national)
                    cursor.execute(f"""SELECT id FROM dim_technologie 
                                    WHERE annee = {year}
                                    LIMIT 1""")
                    result = cursor.fetchone()
                    technologie_id = result[0] if result else None
                    
                    # Ajouter la ligne à fact_data seulement si certaines dimensions sont non nulles
                    if any([id_parti, securite_id, socio_eco_id, sante_id, environnement_id, education_id, demographie_id, technologie_id]):
                        fact_data.append({
                            'annee_code_dpt': annee_code_dpt,
                            'id_parti': id_parti,
                            'securite_id': securite_id,
                            'socio_eco_id': socio_eco_id,
                            'sante_id': sante_id,
                            'environnement_id': environnement_id,
                            'education_id': education_id,
                            'demographie_id': demographie_id,
                            'technologie_id': technologie_id
                        })
            
            # Créer un DataFrame pandas à partir des données de faits
            if fact_data:
                fact_df = pd.DataFrame(fact_data)
                
                # Insérer dans la table de faits
                self.insert_data_to_mysql("fact_resultats_politique", fact_df)
                logger.info(f"✅ {len(fact_data)} lignes insérées dans la table de faits")
            else:
                logger.warning("⚠️ Aucune donnée à insérer dans la table de faits")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de la table de faits: {e}")
            return False
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()
    
                
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
        DEPT_FILE = "data/origine/politique/departements-france.csv"
        TARGET_YEARS = list(range(2000, 2023))  # All years from 2000 to 2022 inclusive
        
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
        