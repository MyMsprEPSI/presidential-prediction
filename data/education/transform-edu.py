import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, year, first, sum, round, lower, trim, lit
from pyspark.sql.types import StringType, IntegerType, DateType

def main():
    # Configuration de Java et Spark
    os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'
    os.environ['SPARK_HOME'] = r'C:\Users\thoma\AppData\Roaming\Python\Python312\site-packages\pyspark'
    os.environ['PATH'] = os.environ['JAVA_HOME'] + r'\bin;' + os.environ['SPARK_HOME'] + r'\bin;' + os.environ['PATH']
    os.environ['PYSPARK_PYTHON'] = r'C:\Python312\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Python312\python.exe'
    
    # Vérification de Java
    try:
        subprocess.run(['java', '-version'], capture_output=True, check=True)
    except FileNotFoundError:
        print("Erreur: Java n'est pas trouvé. Veuillez installer Java 17 et configurer JAVA_HOME.")
        return

    # Initialisation de Spark avec configuration minimale
    spark = SparkSession.builder \
        .appName("TransformationEducation") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .master("local[*]") \
        .getOrCreate()

    # Configuration du niveau de log
    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/education/fr-en-etablissements-fermes.csv"
    output_folder = "data/education/output"

    # Extraction des données
    df = extract_data(spark, input_path)
    
    # Profilage des données
    profile_data(df)
    
    # Transformation et nettoyage des données
    df_transformed = transform_data(df)

    # Calcul du nombre d'établissements fermés par année et par département
    df_grouped = calculate_closed_by_year_and_dept(df_transformed)
    
    # Chargement des données
    load_data(df_grouped, "data/education/output_grouped")
    load_data(df_transformed, output_folder)

    # Arrêt de la session Spark
    spark.stop()

def extract_data(spark: SparkSession, input_path: str):
    """
    Extrait les données depuis un fichier CSV avec PySpark.
    """
    if not os.path.exists(input_path):
        print(f"Erreur: Le fichier {input_path} n'existe pas.")
        return None
    
    try:
        # Lecture du CSV avec PySpark
        df = spark.read \
            .option("header", "true") \
            .option("sep", ";") \
            .option("encoding", "UTF-8") \
            .csv(input_path)
        
        print(f"✓ Données chargées avec succès depuis {input_path}")
        print(f"✓ Nombre de lignes: {df.count()}")
        print(f"✓ Colonnes présentes: {', '.join(df.columns)}")
        
        return df
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement du fichier CSV : {str(e)}")
        return None

def profile_data(df):
    """
    Effectue un profilage détaillé du DataFrame Spark.
    """
    print("\n=== Profilage du DataFrame ===\n")
    
    # Schéma du DataFrame
    print("Schéma du DataFrame :")
    df.printSchema()
    
    # Aperçu des données
    print("\nAperçu des 5 premières lignes :")
    df.show(5, truncate=False)
    
    # Statistiques descriptives
    print("\nStatistiques descriptives :")
    df.describe().show()
    
    # Nombre total de lignes
    total_rows = df.count()
    print(f"\nNombre total de lignes : {total_rows}")
    
    # Analyse des valeurs nulles
    print("\n--- Analyse des valeurs manquantes ---")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percent = (null_count / total_rows) * 100
        print(f"Colonne '{column}': {null_count} valeurs manquantes ({null_percent:.2f}%)")

def transform_data(df):
    """
    Transforme et nettoie les données avec PySpark.
    """
    print("\n=== TRANSFORMATION ET NETTOYAGE DES DONNÉES ===")
    
    # 1. Suppression des doublons
    df = df.dropDuplicates()
    
    # 2. Standardisation des colonnes textuelles
    for column in df.columns:
        df = df.withColumn(column, 
                          when(col(column).isNotNull(),
                               trim(lower(col(column))))
                          .otherwise(lit("non spécifié")))
    
    # 3. Conversion de la date_fermeture et extraction de l'année
    if "date_fermeture" in df.columns:
        df = df.withColumn("date_fermeture", 
                          col("date_fermeture").cast(DateType()))
        df = df.withColumn("annee_fermeture", 
                          year(col("date_fermeture")))
    
    # 4. Normalisation des codes postaux
    if "code_postal" in df.columns:
        df = df.withColumn("code_postal",
                          when(col("code_postal").isNull(), "00000")
                          .otherwise(trim(col("code_postal"))))
    
    # 5. Séparation du secteur public/privé
    if "secteur_public_prive_libe" in df.columns:
        df = df.withColumn("secteur_public", 
                          when(lower(col("secteur_public_prive_libe")) == "public", 1)
                          .otherwise(0))
        df = df.withColumn("secteur_prive", 
                          when(lower(col("secteur_public_prive_libe")) == "privé", 1)
                          .otherwise(0))
    
    print("\n--- Aperçu des données nettoyées ---")
    df.show(5)
    
    return df

def calculate_closed_by_year_and_dept(df):
    """
    Calcule les statistiques par année et département avec PySpark.
    """
    if "annee_fermeture" not in df.columns:
        print("❌ La colonne 'annee_fermeture' est manquante.")
        return None

    # Statistiques générales
    print("\n=== Statistiques sur les fermetures d'établissements ===")
    df.agg({"annee_fermeture": "min"}).show()
    df.agg({"annee_fermeture": "max"}).show()

    # Groupement par année et département
    df_grouped = df.groupBy("annee_fermeture", "code_departement", "libelle_departement") \
        .agg(
            count("numero_uai").alias("nombre_total_etablissements"),
            sum("secteur_public").alias("nb_public"),
            sum("secteur_prive").alias("nb_prive"),
            round((sum("secteur_public") * 100 / count("*")), 2).alias("pct_public"),
            round((sum("secteur_prive") * 100 / count("*")), 2).alias("pct_prive")
        ) \
        .orderBy("annee_fermeture", "code_departement")

    print("\nAperçu des statistiques par département et année :")
    df_grouped.show(10)
    
    return df_grouped

def load_data(df, output_folder):
    """
    Sauvegarde le DataFrame Spark en un seul fichier CSV.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    try:
        # Utilisation de coalesce(1) pour forcer la sortie en un seul fichier
        (df.coalesce(1)
           .write
           .mode("overwrite")  # Écrase les fichiers existants
           .option("header", "true")
           .option("sep", ";")
           .option("encoding", "UTF-8")
           .format("csv")  # Spécifie explicitement le format
           .save(output_folder))
        
        # Renommer le fichier part-* en data_transformed.csv
        for filename in os.listdir(output_folder):
            if filename.startswith("part-") and filename.endswith(".csv"):
                old_file = os.path.join(output_folder, filename)
                new_file = os.path.join(output_folder, "data_transformed.csv")
                os.rename(old_file, new_file)
                break
                
        print(f"✓ Données sauvegardées dans {output_folder}/data_transformed.csv avec le séparateur point-virgule (;)")
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des données : {e}")

if __name__ == "__main__":
    main()
