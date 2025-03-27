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
    logger.info("🚀 Démarrage du processus ETL")

    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    logger.info("📁 Configuration des chemins de fichiers...")
    input_file_path = (
        "./data/environnemental/parc-regional-annuel-prod-eolien-solaire.csv"
    )

    pib_files = [
        "./data/socio-economie/PIB Guadeloupe.csv",
        "./data/socio-economie/PIB Martinique.csv",
        "./data/socio-economie/PIB Guyane.csv",
        "./data/socio-economie/PIB La Réunion.csv",
        "./data/socio-economie/PIB Mayotte.csv",  # temporairement commenté par manque de données
    ]

    region_codes = {
        "./data/socio-economie/PIB Guadeloupe.csv": "01",
        "./data/socio-economie/PIB Martinique.csv": "02",
        "./data/socio-economie/PIB Guyane.csv": "03",
        "./data/socio-economie/PIB La Réunion.csv": "04",
        "./data/socio-economie/PIB Mayotte.csv": "06",
    }

    # Ajoute à tes imports existants
    pib_xlsx_file = "./data/socio-economie/PIB 1990 - 2021.xlsx"
    pib_2022_file = "./data/socio-economie/PIB par Région en 2022.csv"
    inflation_xlsx_file = "./data/socio-economie/Essentiel_Inflation_donnees.xlsx"

    # Ajouter dans la section des chemins de fichiers
    technologie_xlsx_file = "./data/technologie/Effort-recherche_tableaux_2024.xlsx"
    election_files_pattern = (
        "./data/politique/taux-votes/1965_2012/cdsp_presi*t2_circ.csv"
    )
    election_2017_file = (
        "./data/politique/taux-votes/2017/Presidentielle_2017_Resultats_Tour_2_c.xls"
    )
    election_2022_file = "./data/politique/taux-votes/2022/resultats-par-niveau-subcom-t2-france-entiere.xlsx"

    demo_file = "./data/demographie/estim-pop-dep-sexe-gca-1975-2023.xls"
    education_file = "./data/education/fr-en-etablissements-fermes.csv"
    life_expectancy_file = "./data/sante/valeurs_annuelles.csv"
    departments_file_path = "./data/departements-france.csv"

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
    # 3.4) EXTRACT : Charger les données de technologie
    # ----------------------------------------------------------------
    df_technologie = extractor.extract_technologie_data(technologie_xlsx_file)

    if df_technologie is None:
        logger.error("❌ Extraction des données de technologie échouée.")
        return

    # ----------------------------------------------------------------
    # 3.5) EXTRACT : Charger les données électorales
    # ----------------------------------------------------------------
    df_election_1965_2012 = extractor.extract_election_data_1965_2012(
        election_files_pattern
    )
    df_election_2017 = extractor.extract_election_data_2017(election_2017_file)
    df_election_2022 = extractor.extract_election_data_2022(election_2022_file)

    # ----------------------------------------------------------------
    # 3.6) EXTRACT : Charger les données de démographie
    # ----------------------------------------------------------------
    df_demographie = extractor.extract_demographic_data(demo_file)
    if df_demographie is None:
        logger.error("❌ Extraction des données démographiques échouée.")
    else:
        logger.info("✅ Extraction des données démographiques réussie")
        df_demographie.show(5, truncate=False)
    # ----------------------------------------------------------------
    # 3.7) EXTRACT : Charger les données d'éducation
    # ----------------------------------------------------------------
    df_education = extractor.extract_education_data(education_file)
    if df_education is None:
        logger.error("❌ Échec de l'extraction des données d'éducation")
        return
    logger.info("✅ Extraction des données d'éducation réussie")
    df_education.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 3.8) EXTRACT : Charger les données de sécurité
    # ----------------------------------------------------------------


    # ----------------------------------------------------------------
    # 3.9) EXTRACT : Charger les données de santé
    # ----------------------------------------------------------------

    df_life_expectancy_raw = extractor.extract_life_expectancy_data(life_expectancy_file)

    if df_life_expectancy_raw is None:
        logger.error("❌ Échec de l'extraction des données. Arrêt du programme.")
        return

    logger.info("✅ Extraction réussie ! Aperçu des données extraites :")
    df_life_expectancy_raw.show(3, truncate=False)

    # Extraction des données de départements
    df_departments = extractor.extract_departments_data(departments_file_path)
    if df_departments is None:
        logger.error("❌ Échec de l'extraction des données des départements.")
        return
    logger.info("✅ Extraction des données des départements réussie.")
    df_departments.show(5, truncate=False)



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
    df_pib_transformed_completed = transformer.fill_missing_pib_mayotte(
        df_pib_transformed
    )

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

    df_pib_inflation = transformer.combine_pib_and_inflation(
        df_pib_total, df_inflation_transformed
    )

    if df_pib_inflation is None:
        logger.error("❌ Fusion PIB + Inflation échouée.")
        return

    logger.info("✅ Fusion des données PIB et Inflation réussie :")
    df_pib_inflation.show(10, truncate=False)

    # ----------------------------------------------------------------
    # 4.4) TRANSFORM : Transformation des données de technologie
    # ----------------------------------------------------------------
    df_technologie_transformed = transformer.transform_technologie_data(df_technologie)

    if df_technologie_transformed is None:
        logger.error("❌ Transformation des données de technologie échouée.")
        return

    # ----------------------------------------------------------------
    # 4.5) TRANSFORM : Transformation des données électorales
    # ----------------------------------------------------------------
    # Transformation des données 1965-2012
    df_election_1965_2012_transformed = transformer.transform_election_data_1965_2012(
        df_election_1965_2012
    )
    if df_election_1965_2012_transformed is None:
        logger.error("❌ Transformation des données électorales 1965-2012 échouée.")
        return

    # Transformation des données 2017
    df_election_2017_transformed = transformer.transform_election_data_2017(
        df_election_2017
    )
    if df_election_2017_transformed is None:
        logger.error("❌ Transformation des données électorales 2017 échouée.")
        return

    # Transformation des données 2022
    df_election_2022_transformed = transformer.transform_election_data_2022(
        df_election_2022
    )
    if df_election_2022_transformed is None:
        logger.error("❌ Transformation des données électorales 2022 échouée.")
        return

    # Combinaison de toutes les années
    df_election_final = transformer.combine_all_years(
        df_election_1965_2012_transformed,
        df_election_2017_transformed,
        df_election_2022_transformed,
    )

    if df_election_final is None:
        logger.error("❌ Combinaison des données électorales échouée.")
        return

    logger.info("✅ Transformation des données électorales réussie !")
    df_election_final.show(10, truncate=False)

    # ----------------------------------------------------------------
    # 4.6) TRANSFORMATION : nettoyage et sélection des données démographiques
    # ----------------------------------------------------------------
        # Supposons que df_demographie est déjà transformé ou brut, selon votre pipeline
    if df_demographie is not None:
        # Séparons totaux et départements
        df_totaux, df_departements = transformer.separate_demographic_totals(df_demographie)
        
        # Enregistrer chaque partie dans un fichier CSV distinct
        #  - "population_totaux.csv"
        #  - "population_par_departement.csv"
        # On suppose qu'on utilise le loader pour ça :
        
        loader.save_to_csv(df_totaux, "population_totaux.csv")
        loader.save_to_csv(df_departements, "population_par_departement.csv")
        
        logger.info("✅ Données démographiques enregistrées séparément (totaux / départements).")
    else:
        logger.error("❌ Pas de données démographiques à séparer.")

    
    # ----------------------------------------------------------------
    # 4.7) TRANSFORMATION : nettoyage et sélection des données d'éducation
    # ----------------------------------------------------------------

    df_edu_clean = transformer.transform_education_data(df_education)
    logger.info("✅ Transformation et nettoyage des données d'éducation réussie")
    df_edu_clean.show(5, truncate=False)

    # 4.7.1) Calcul des statistiques par année et département
    df_edu_grouped = transformer.calculate_closed_by_year_and_dept_education(df_edu_clean)
    if df_edu_grouped is None:
        logger.error("❌ Échec du calcul des statistiques d'éducation")
        return
    logger.info("✅ Calcul des statistiques terminé")
    df_edu_grouped.show(10, truncate=False)

    # ----------------------------------------------------------------
    # 4.8) TRANSFORMATION : nettoyage et sélection des données de sécurité
    # ----------------------------------------------------------------


    # ----------------------------------------------------------------
    # 4.9) TRANSFORM : nettoyage et sélection des données de santé
    # ----------------------------------------------------------------

    # Transformation des données d'espérance de vie en incluant la jointure avec les départements
    df_life_final = transformer.transform_life_expectancy_data(df_life_expectancy_raw, df_departments)
    if df_life_final is None:
        logger.error("❌ Échec de la transformation des données d'espérance de vie.")
        return
    logger.info("✅ Transformation des données d'espérance de vie réussie.")
    df_life_final.show(10, truncate=False)

    df_life_final = transformer.fill_missing_mayotte_life_expectancy(df_life_final)


    # ----------------------------------------------------------------
    # 5) LOAD : Sauvegarde en fichier CSV
    # ----------------------------------------------------------------
    loader.save_to_csv(df_transformed, input_file_path)

    loader.save_to_csv(df_edu_grouped, education_file)

    output_path_final = "pib_inflation_final.csv"
    loader.save_to_csv(df_pib_inflation, output_path_final)

    # Ajouter dans la section LOAD
    output_path_technologie = "technologie_pib_france_1990_2023.csv"
    loader.save_to_csv(df_technologie_transformed, output_path_technologie)

    # Dans la section LOAD, ajoutez :
    output_path_election = "vote_presidentiel_par_dept_1965_2022.csv"
    loader.save_to_csv(df_election_final, output_path_election)

    output_path_life = "esperance_de_vie_par_departement_2000_2022.csv"
    loader.save_to_csv(df_life_final, output_path_life)

    # ----------------------------------------------------------------
    # 5.6) LOAD : Sauvegarde des données démographiques
    # ----------------------------------------------------------------
    loader.save_to_csv(df_totaux, "./data/demographie/population_totaux.csv")
    loader.save_to_csv(df_departements, "./data/demographie/population_par_departement.csv")

    # ----------------------------------------------------------------
    # 6) Arrêt de la session Spark
    # ----------------------------------------------------------------
    extractor.stop()


if __name__ == "__main__":
    try:
        main()
        logger.info("✅ Processus ETL terminé avec succès")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution du processus ETL : {str(e)}")
        raise
