# main.py
import os
from pyspark.sql import SparkSession
from extract_demo import extract_data
from transform_demo import transform_demographie_data, separate_totals, final_cleaning_totaux
from load_demo import write_spark_output, merge_spark_output

def main():
    # Création de la session Spark
    spark = SparkSession.builder \
        .appName("ETL_Demographie") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Chemin du fichier Excel source (doit être au format XLSX)
    excel_path = "D:\\Thomas\\Documents\\GitHub\\bigdata-project\\data\\demographie\\estim-pop-dep-sexe-gca-1975-2023.xls"
    
    # Extraction des données depuis le fichier Excel
    print("🚀 Démarrage de l'extraction...")
    df_extracted = extract_data(spark, excel_path)
    
    # Transformation des données
    print("🚀 Démarrage de la transformation...")
    df_transformed = transform_demographie_data(df_extracted)
    
    # Séparation des totaux et des données départementales
    df_totaux, df_departements = separate_totals(df_transformed)
    # Nettoyage final sur le DataFrame des totaux
    df_totaux = final_cleaning_totaux(df_totaux)
    
    # Chemins de sortie
    output_dir_departements = "./data/output/dep_demo/"
    output_dir_totaux = "/data/output/totaux_demo/"
    output_file_departements = "./data/output/dep_demo/population_par_departement_spark.csv"
    output_file_totaux = "/data/output/totaux_demo/population_totaux_spark.csv"
    
    # Chargement des données transformées
    print("🚀 Démarrage du chargement des données départementales...")
    write_spark_output(df_departements, output_dir_departements)
    print("🚀 Démarrage du chargement des données totaux...")
    write_spark_output(df_totaux, output_dir_totaux)
    
    # Fusion des sorties CSV en un fichier final par ensemble
    merge_spark_output(output_dir_departements, output_file_departements)
    merge_spark_output(output_dir_totaux, output_file_totaux)
    
    # Arrêt de la session Spark
    spark.stop()
    print("🚀 ETL terminé.")

if __name__ == "__main__":
    main()
