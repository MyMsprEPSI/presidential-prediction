import logging
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.loader import DataLoader
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Démarrage du test pour la dimension sécurité")

    # Création de la session Spark
    spark = SparkSession.builder.getOrCreate()
    
    try:
        # Initialisation des composants ETL et du loader
        extractor = DataExtractor()
        transformer = DataTransformer()
        loader = DataLoader(spark)  # On transmet la session Spark au DataLoader
    
        # Chemin du fichier source pour la sécurité
        security_file = "./data/securite/tableaux-4001-ts.xlsx"
    
        # Extraction des données de sécurité via l'extracteur
        df_security = extractor.extract_security_data(security_file)
        if df_security is None:
            logger.error("❌ Échec de l'extraction des données de sécurité.")
            return
    
        # Transformation des données de sécurité
        df_security_transformed = transformer.transform_security_data(df_security)
        if df_security_transformed is None:
            logger.error("❌ Échec de la transformation des données de sécurité.")
            return
    
        # Préparation des données pour la dimension sécurité
        dim_securite = transformer.prepare_dim_securite(df_security_transformed)
        if dim_securite is None:
            logger.error("❌ Échec de la préparation de la dimension sécurité.")
            return
    
        # Insertion directe dans la base de données (ici, mode "append")
        insertion_ok = loader.load_dim_securite(dim_securite, mode="append")
        if insertion_ok:
            logger.info("✅ Insertion des données dans dim_securite réussie")
        else:
            logger.error("❌ Échec de l'insertion des données dans dim_securite")
    
    except Exception as e:
        logger.error(f"❌ Erreur lors du test de la dimension sécurité : {e}")
    
    finally:
        spark.stop()
        logger.info("🚀 Fin du test pour la dimension sécurité")

if __name__ == "__main__":
    main()
