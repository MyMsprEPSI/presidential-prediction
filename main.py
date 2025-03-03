import os
import logging
from etl.extract import DataExtractor
from etl.transform import DataTransformer

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Point d'entrée principal de l'ETL.
    Gère l'extraction, la transformation et le chargement des données.
    """

    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    data_path = "./data/environnemental/parc-regional-annuel-prod-eolien-solaire.csv"

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    extractor = DataExtractor()
    transformer = DataTransformer()

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données
    # ----------------------------------------------------------------
    df_environnemental = extractor.extract_environmental_data(data_path)

    if df_environnemental:
        logger.info("✅ Extraction réussie ! Aperçu des données :")
        df_environnemental.show(5)  # Afficher un aperçu des 5 premières lignes

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Transformer les données
    # ----------------------------------------------------------------
    if df_environnemental:
        df_environnemental = transformer.transform_environmental_data(df_environnemental)
        

    # Arrêt de la session Spark
    extractor.stop()


if __name__ == "__main__":
    main()
