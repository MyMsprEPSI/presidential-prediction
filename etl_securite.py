import os
import pandas as pd
from pyspark.sql import SparkSession

# Définir les chemins de fichiers
file_csv = r"C:\Users\joyce\EPSI\MSPR\Bloc3\MSPR\datas\Securite\donnee-dep-data.gouv-2024-geographie2024-produit-le2025-01-26.csv"
file_excel = r"C:\Users\joyce\EPSI\MSPR\Bloc3\MSPR\datas\Securite\nombre_homicide.xlsx"

# Vérifier si les fichiers existent
for file in [file_csv, file_excel]:
    if not os.path.exists(file):
        raise FileNotFoundError(f"Fichier introuvable : {file}")

# Charger les fichiers avec Pandas
df_csv = pd.read_csv(file_csv, encoding='utf-8')
df_excel = pd.read_excel(file_excel, engine='openpyxl')

# Afficher un aperçu
print(" Aperçu du fichier CSV :")
print(df_csv.head())

print("Aperçu du fichier Excel :")
print(df_excel.head())

# Fusionner les fichiers avec Pandas (ajuste selon la colonne commune)
merged_df = pd.merge(df_csv, df_excel, on="id", how="inner")

# Sauvegarder le fichier fusionné en CSV
output_file = r"C:\Users\joyce\EPSI\MSPR\Bloc3\MSPR\datas\Securite\donnees_fusionnees.csv"
merged_df.to_csv(output_file, index=False, encoding='utf-8')

print(f"\n Fichier fusionné enregistré : {output_file}")

# ===============================================
# OPTION 2 : Nettoyage ETL avec Spark
# ===============================================

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("nombre_de_condamnation_homicide") \
    .getOrCreate()

# Définir les fichiers pour Spark
file_spark_1 = r"C:\Users\joyce\OneDrive\Desktop\donnee-dep-data.gouv-2024-geographie2024-produit-le2025-01-26.csv"
file_spark_2 = r"C:\Users\joyce\OneDrive\Desktop\nombre_homicide.csv"

# Vérifier si les fichiers existent
for file in [file_spark_1, file_spark_2]:
    if not os.path.exists(file):
        raise FileNotFoundError(f"Fichier introuvable pour Spark : {file}")

# Lire les fichiers avec Spark
df1 = spark.read.csv(file_spark_1, header=True, inferSchema=True)
df2 = spark.read.csv(file_spark_2, header=True, inferSchema=True)

# Afficher les schémas pour vérifier les colonnes
print("\n Schéma du fichier CSV 1 :")
df1.printSchema()

print("\n Schéma du fichier CSV 2 :")
df2.printSchema()

# Fusionner les DataFrames sur les colonnes communes
merged_df = df1.join(df2, on=["Code_departement", "Code_region", "annee", "indicateur", "unite_de_compte", "nombre"], how="inner")

# Nettoyage : Supprimer les valeurs nulles
merged_df = merged_df.dropna()

# Nettoyage : Supprimer les doublons
merged_df = merged_df.dropDuplicates()

# Sauvegarder le DataFrame fusionné en CSV
output_spark = r"C:\Users\joyce\OneDrive\Desktop\nombre_de_condamnation_homicide.csv"
merged_df.write.csv(output_spark, header=True, mode="overwrite")

print(f"\n Fichier Spark nettoyé enregistré : {output_spark}")

# Arrêter Spark
spark.stop()
