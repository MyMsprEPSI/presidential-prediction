import os
import pandas as pd
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, expr

# --------------------------------------------------------
# 1️⃣ Conversion du fichier XLS en XLSX avec Pandas
# --------------------------------------------------------

file_xls = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/estim-pop-dep-sexe-gca-1975-2023.xls"
file_xlsx = file_xls.replace(".xls", ".xlsx")

if not os.path.exists(file_xlsx):  # Éviter de convertir plusieurs fois
    print("📥 Conversion du fichier XLS en XLSX...")
    df = pd.read_excel(file_xls, sheet_name=None)  # Charger toutes les feuilles

    with pd.ExcelWriter(file_xlsx, engine="openpyxl") as writer:
        for sheet, data in df.items():
            data.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ Conversion réussie : {file_xlsx}")

# --------------------------------------------------------
# 2️⃣ Extraction et fusion des données dans un seul CSV
# --------------------------------------------------------

file_csv = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_par_departement_annee.csv"

if not os.path.exists(file_csv):  # Éviter de recréer plusieurs fois
    print("📥 Extraction et fusion des feuilles Excel...")

    # Charger toutes les feuilles sauf la première ("À savoir")
    xls = pd.ExcelFile(file_xlsx)
    sheets_to_read = xls.sheet_names[1:]  # Exclure la première feuille

    dfs = []
    for sheet in sheets_to_read:
        print(f"📄 Traitement de la feuille : {sheet}")
        df = pd.read_excel(xls, sheet_name=sheet, skiprows=3)  # Sauter les 3 premières lignes inutiles
        df["Année"] = sheet  # Ajouter la colonne Année
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    # Sauvegarde en CSV
    df_final.to_csv(file_csv, index=False, sep=";")
    print(f"✅ Fichier CSV généré : {file_csv}")

# --------------------------------------------------------
# 3️⃣ Traitement avec PySpark
# --------------------------------------------------------

# --------------------------------------------------------
# 4️⃣ Traitement avec PySpark
# --------------------------------------------------------

print("🚀 Lancement du traitement PySpark...")
spark = SparkSession.builder.appName("Traitement_Population").getOrCreate()

df_spark = spark.read.option("header", True).option("sep", ";").csv(file_csv)

# Renommer les colonnes avec les bons en-têtes
df_spark = df_spark.withColumnRenamed("F 0-19 ans", "F_0_19_ans")\
    .withColumnRenamed("F 20-39 ans", "F_20_39_ans")\
    .withColumnRenamed("F 40-59 ans", "F_40_59_ans")\
    .withColumnRenamed("F 60-74 ans", "F_60_74_ans")\
    .withColumnRenamed("F 75 ans et plus", "F_75_et_plus")\
    .withColumnRenamed("F Total", "F_Total")\
    .withColumnRenamed("H 0-19 ans", "H_0_19_ans")\
    .withColumnRenamed("H 20-39 ans", "H_20_39_ans")\
    .withColumnRenamed("H 40-59 ans", "H_40_59_ans")\
    .withColumnRenamed("H 60-74 ans", "H_60_74_ans")\
    .withColumnRenamed("H 75 ans et plus", "H_75_et_plus")\
    .withColumnRenamed("H Total", "H_Total")\
    .withColumnRenamed("Total", "Population_Totale")

# Supprimer la 2ème ligne (les intitulés en double) et les lignes de source/notes
df_spark = df_spark.where(
    (~col("Code_Département").startswith("Source")) &
    (~col("Code_Département").startswith("NB:")) &
    (~col("Code_Département").contains("("))
)

# Convertir la colonne Code_Département en numérique
df_spark = df_spark.withColumn(
    "Code_Département_Num",
    when(col("Code_Département") == "2A", 201)  # Corse-du-Sud
    .when(col("Code_Département") == "2B", 202)  # Haute-Corse
    .otherwise(col("Code_Département").cast("int"))
)

# Trier par année (décroissant) puis par code département
df_spark = df_spark.orderBy(col("Année").desc(), "Code_Département_Num")

# Séparer les totaux France Métro et DOM-TOM
df_totaux = df_spark.filter(
    (col("Code_Département").contains("France")) | (col("Code_Département").contains("DOM"))
)

# Filtrer les départements uniquement
df_departements = df_spark.subtract(df_totaux)

# 🔹 Écraser les anciens fichiers
dir_departements = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_par_departement_spark"
dir_totaux = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_totaux_spark"

shutil.rmtree(dir_departements, ignore_errors=True)
shutil.rmtree(dir_totaux, ignore_errors=True)

df_departements.write.option("header", True).option("sep", ";").mode("overwrite").csv(dir_departements)
df_totaux.write.option("header", True).option("sep", ";").mode("overwrite").csv(dir_totaux)

# 🔹 Fusion des fichiers en un seul CSV propre 🔹
file_departements = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_par_departement_spark.csv"
file_totaux = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_totaux_spark.csv"

def overwrite_file(file_path):
    """Supprime le fichier s'il existe déjà"""
    if os.path.exists(file_path):
        os.remove(file_path)

overwrite_file(file_departements)
overwrite_file(file_totaux)

def merge_spark_output(spark_output_dir, output_file):
    """ Fusionne les fichiers Spark en un seul CSV """
    with open(output_file, "w", encoding="utf-8") as outfile:
        for i, part_file in enumerate(sorted(os.listdir(spark_output_dir))):
            part_path = os.path.join(spark_output_dir, part_file)
            if part_file.startswith("part-"):  # Ignorer les fichiers _SUCCESS
                with open(part_path, "r", encoding="utf-8") as infile:
                    if i == 0:  # Garde l'en-tête du premier fichier uniquement
                        outfile.write(infile.read())
                    else:
                        infile.readline()  # Supprime la première ligne (header)
                        outfile.write(infile.read())

    shutil.rmtree(spark_output_dir)  # Supprime les fichiers intermédiaires

# Fusionner les résultats
merge_spark_output(dir_departements, file_departements)
merge_spark_output(dir_totaux, file_totaux)

print(f"✅ Traitement terminé :\n - {file_departements} (données par département)\n - {file_totaux} (totaux France)")

spark.stop()