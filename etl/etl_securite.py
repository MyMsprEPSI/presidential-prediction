from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("ETL Process with PySpark") \
    .getOrCreate()

def extract_data(file_path, file_type='csv'):
    if file_type == 'csv':
        return spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
    elif file_type == 'parquet':
        return spark.read.parquet(file_path)
    else:
        raise ValueError("Unsupported file type")

def transform_data(df):
    # Exemple de transformation : filtrer les lignes avec des valeurs nulles
    if 'code_region' in df.columns:
        df = df.drop('code_region')

    df_cleaned = df.dropna()
    return df_cleaned

def load_data(df, output_path):
    df.write.csv(output_path, header=True)
    print(f"Data successfully loaded to {output_path}")

def main():
    # Chemins des fichiers d'entrée et de sortie
    input_file_path = 'c:/Users/joyce/EPSI/MSPR/Bloc 3/MSPR/datas/Securite/nombre_de_condamnation_homicide.csv'  # Remplacez par votre chemin
    output_file_path = 'c:/Users/joyce/EPSI/MSPR/Bloc 3/MSPR/datas/Securite/'  # Remplacez par votre chemin

    # Extraction
    data = extract_data(input_file_path, file_type='csv')

    # Transformation
    transformed_data = transform_data(data)

    # Chargement
    load_data(transformed_data, output_file_path)

if __name__ == "__main__":
    main()
