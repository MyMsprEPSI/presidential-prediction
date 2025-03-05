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
    """
    Point d'entrée principal de l'ETL :
    - Extraction des données
    - Transformation
    - Enregistrement en CSV
    """

    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    input_file_path = (
        "./data/environnemental/parc-regional-annuel-prod-eolien-solaire.csv"
    )

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données
    # ----------------------------------------------------------------
    df_env = extractor.extract_environmental_data(input_file_path)

    if df_env is None:
        logger.error("❌ Échec de l'extraction des données. Arrêt du programme.")
        return

    logger.info("✅ Extraction réussie ! Aperçu des données extraites :")
    df_env.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Nettoyage et sélection des données
    # ----------------------------------------------------------------
    df_transformed = transformer.transform_environmental_data(df_env)

    if df_transformed is None:
        logger.error("❌ Échec de la transformation des données. Arrêt du programme.")
        return

    logger.info("✅ Transformation réussie ! Aperçu des données transformées :")
    df_transformed.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 5) LOAD : Sauvegarde en fichier CSV
    # ----------------------------------------------------------------
    loader.save_to_csv(df_transformed, input_file_path)

    # ----------------------------------------------------------------
    # 6) Arrêt de la session Spark
    # ----------------------------------------------------------------
    extractor.stop()


if __name__ == "__main__":
    main()
