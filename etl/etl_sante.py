from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("ETL Life Expectancy Data") \
    .getOrCreate()

def extract_data(file_path, file_type='csv'):
    if file_type == 'csv':
        # Spécifier le délimiteur comme étant un point-virgule
        return spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    elif file_type == 'parquet':
        return spark.read.parquet(file_path)
    else:
        raise ValueError("Unsupported file type")

def transform_data(df):
    # Sélectionner uniquement les colonnes nécessaires
    selected_columns = ['Code_departement', 'Departement', 'Annee', 'esperance_de_vie_hommes', 'esperance_de_vie_femmes']
    df = df.select(*selected_columns)
    
    # Filtrer les valeurs nulles
    df_cleaned = df.dropna()
    return df_cleaned

def load_data(df, output_path):
    df.write.csv(output_path, header=True, mode='overwrite')
    print(f"Data successfully loaded to {output_path}")

def main():
    # Chemins des fichiers d'entrée et de sortie
    input_file_path = "C:\\Users\\joyce\\EPSI\\MSPR\\Bloc 3\\MSPR\\datas\\santé\\esperance_de_vie_2000_2022.xlsx"
    output_file_path = 'C:\\Users\\joyce\\EPSI\\MSPR\\Bloc 3\\MSPR\\datas\\santé' 

    # Extraction
    data = extract_data(input_file_path, file_type='csv')

    # Transformation
    transformed_data = transform_data(data)

    # Chargement
    load_data(transformed_data, output_file_path)

if __name__ == "__main__":
    main()
