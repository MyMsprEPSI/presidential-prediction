# main.py

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
        "./data/socio-economie/PIB Mayotte.csv" # temporairement commenté par manque de données
    ]

    region_codes = {
        "./data/socio-economie/PIB Guadeloupe.csv": "01",
        "./data/socio-economie/PIB Martinique.csv": "02",
        "./data/socio-economie/PIB Guyane.csv": "03",
        "./data/socio-economie/PIB La Réunion.csv": "04",
        "./data/socio-economie/PIB Mayotte.csv": "06"
    }

    # Ajoute à tes imports existants
    pib_xlsx_file = "./data/socio-economie/PIB 1990 - 2021.xlsx"
    pib_2022_file = "./data/socio-economie/PIB par Région en 2022.csv"
    inflation_xlsx_file = "./data/socio-economie/Essentiel_Inflation_donnees.xlsx"


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
    # 3.2) EXTRACT : Charger les données PIB régionaux (nouveaux fichiers)
    # ----------------------------------------------------------------
    df_pib_xlsx = extractor.extract_pib_excel(pib_xlsx_file)
    df_pib_2022 = extractor.extract_pib_2022(pib_2022_file)

    if df_pib_xlsx is None or df_pib_2022 is None:
        logger.error("❌ Extraction PIB Excel échouée.")
        return
    
    # ----------------------------------------------------------------
    # 3.3) EXTRACT : Charger les données d'inflation
    # ----------------------------------------------------------------
    
    df_inflation = extractor.extract_inflation_data(inflation_xlsx_file)

    if df_inflation is None:
        logger.error("❌ Extraction Inflation échouée.")
        return

    logger.info("✅ Extraction Inflation réussie :")
    df_inflation.show(5, truncate=False)


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

    # Remplissage des valeurs manquantes pour Mayotte uniquement
    df_pib_transformed_completed = transformer.fill_missing_pib_mayotte(df_pib_transformed)

    if df_pib_transformed_completed is None:
        logger.error("❌ Remplissage PIB Mayotte échoué.")
        return

    logger.info("✅ Transformation PIB réussie (avec Mayotte rempli) :")
    df_pib_transformed_completed.show(10, truncate=False)

    # ----------------------------------------------------------------
    # 4.2) TRANSFORM : Combinaison finale de toutes les données PIB
    # ----------------------------------------------------------------
    df_pib_total = transformer.combine_all_pib_data(
        df_pib_transformed_completed, df_pib_xlsx, df_pib_2022
    )

    if df_pib_xlsx is None or df_pib_2022 is None:
        logger.error("❌ Transformation PIB fichiers XLSX ou 2022 échouée.")
        return

    logger.info("✅ Fusion des données PIB réussie :")
    df_pib_total.show(300, truncate=False)

    # ----------------------------------------------------------------
    # 4.3) TRANSFORM : Nettoyage et sélection des données d'inflation
    # ----------------------------------------------------------------
    df_inflation_transformed = transformer.transform_inflation_data(df_inflation)

    if df_inflation_transformed is None:
        logger.error("❌ Transformation Inflation échouée.")
        return


    df_pib_inflation = transformer.combine_pib_and_inflation(df_pib_total, df_inflation_transformed)

    if df_pib_inflation is None:
        logger.error("❌ Fusion PIB + Inflation échouée.")
        return

    logger.info("✅ Fusion des données PIB et Inflation réussie :")
    df_pib_inflation.show(10, truncate=False)
    # ----------------------------------------------------------------
    # 5) LOAD : Sauvegarde en fichier CSV
    # ----------------------------------------------------------------
    loader.save_to_csv(df_transformed, input_file_path)

    output_path_final = "./data/processed_data/pib_inflation_final.csv"
    loader.save_to_csv(df_pib_inflation, output_path_final)

    # ----------------------------------------------------------------
    # 6) Arrêt de la session Spark
    # ----------------------------------------------------------------
    extractor.stop()


if __name__ == "__main__":
    main()
