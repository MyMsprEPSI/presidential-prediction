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

    pib_files = [
        "./data/socio-economie/PIB Guadeloupe.csv",
        "./data/socio-economie/PIB Martinique.csv",
        "./data/socio-economie/PIB Guyane.csv",
        "./data/socio-economie/PIB La Réunion.csv",
        # "./data/socio-economie/PIB Mayotte.csv" # temporairement commenté par manque de données
    ]

    region_codes = {
        "./data/socio-economie/PIB Guadeloupe.csv": "01",
        "./data/socio-economie/PIB Martinique.csv": "02",
        "./data/socio-economie/PIB Guyane.csv": "03",
        "./data/socio-economie/PIB La Réunion.csv": "04",
        # "./data/socio-economie/PIB Mayotte.csv": "06"
    }

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données : environnementales
    # ----------------------------------------------------------------
    df_env = extractor.extract_environmental_data(input_file_path)

    if df_env is None:
        logger.error("❌ Échec de l'extraction des données. Arrêt du programme.")
        return

    logger.info("✅ Extraction réussie ! Aperçu des données extraites :")
    df_env.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 3.1) EXTRACT : Charger les données : PIB des régions d'outre-mer
    # ----------------------------------------------------------------

    df_pib = extractor.extract_pib_outre_mer(pib_files)

    if df_pib is None:
        logger.error("❌ Extraction PIB échouée.")
        return

    logger.info("✅ Extraction PIB réussie :")
    df_pib.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Nettoyage et sélection des données : environnementales
    # ----------------------------------------------------------------
    df_transformed = transformer.transform_environmental_data(df_env)

    if df_transformed is None:
        logger.error("❌ Échec de la transformation des données. Arrêt du programme.")
        return

    logger.info("✅ Transformation réussie ! Aperçu des données transformées :")
    df_transformed.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4.1) TRANSFORM : Nettoyage et sélection des données : PIB des régions d'outre-mer
    # ----------------------------------------------------------------

    df_pib_transformed = transformer.transform_pib_outre_mer(df_pib, region_codes)


    if df_pib_transformed is None:
        logger.error("❌ Transformation PIB échouée.")
        return

    logger.info("✅ Transformation PIB réussie :")
    df_pib_transformed.show(10, truncate=False)

    # ----------------------------------------------------------------
    # 5) LOAD : Sauvegarde en fichier CSV
    # ----------------------------------------------------------------
    loader.save_to_csv(df_transformed, input_file_path)

    output_path_pib = "./data/processed_data/pib_outre_mer.csv"
    loader.save_to_csv(df_pib_transformed, output_path_pib)

    # ----------------------------------------------------------------
    # 6) Arrêt de la session Spark
    # ----------------------------------------------------------------
    extractor.stop()


if __name__ == "__main__":
    main()
