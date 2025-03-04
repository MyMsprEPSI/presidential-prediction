# main.py
import os
import logging
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.loader import DataLoader

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    data_path = "./data/environnemental/parc-regional-annuel-prod-eolien-solaire.csv"

    # JDBC MySQL
    jdbc_url = "jdbc:mysql://localhost:3306/bigdata_project"
    jdbc_user = "root"
    jdbc_password = ""
    jdbc_driver = "com.mysql.cj.jdbc.Driver"

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    extractor = DataExtractor(app_name="BigDataProject_ETL")
    transformer = DataTransformer()
    loader = DataLoader(
        jdbc_url=jdbc_url,
        user=jdbc_user,
        password=jdbc_password,
        database="bigdata_project",
        host="localhost",
        port="3306",
        driver=jdbc_driver,
    )

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données
    # ----------------------------------------------------------------
    logger.info("=== Extraction des données environnementales ===")
    df_env = extractor.extract_environmental_data(data_path)

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Appliquer les transformations
    # ----------------------------------------------------------------
    if df_env is not None:
        df_env_transformed = transformer.transform_environmental_data(df_env)
    else:
        df_env_transformed = None

    # ----------------------------------------------------------------
    # 5) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("=== Chargement des données dans MySQL ===")
    if df_env_transformed is not None:
        loader.load_data(df_env_transformed, "environnement", mode="append")

    # ----------------------------------------------------------------
    # 6) Arrêt de Spark
    # ----------------------------------------------------------------
    extractor.stop()
    logger.info("✅ ETL terminé avec succès.")


if __name__ == "__main__":
    main()
