from pyspark.sql import SparkSession, functions as F , types as T
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    udf , col , regexp_extract , lit , when , expr,
    concat_ws, concat , split , regexp_replace , lower,
    trim , upper , coalesce, collect_list
)
import re
import glob
import sys

# Création de la session Spark
spark = SparkSession.builder.appName("PresidentSummary") \
                            .master("local[*]") \
                            .config("spark.driver.host", "127.0.0.1") \
                            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                            .config("spark.network.timeout", "600s") \
                            .config("spark.executor.heartbeatInterval", "60s") \
                            .config("spark.driver.memory", "4g") \
                            .config("spark.executor.memory", "4g") \
                            .config(
                                "spark.driver.extraClassPath",
                                "./database/connector/mysql-connector-j-9.1.0.jar;./database/connector/spark-excel_2.12-3.5.0_0.20.3.jar",
                            ) \
                            .getOrCreate()

# # Chargement de tous les fichiers CSV (les noms respectent le motif "cdsp_presi*t2_circ.csv")
# file_list  = glob.glob("./data/politique/taux-votes/1965_2012/cdsp_presi*t2_circ.csv")

# results = []

# for file in file_list:
#     # Lecture du fichier CSV avec en-tête et schéma inféré
#     df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)
    
#     # Ajout d'une colonne pour extraire l'année depuis le nom du fichier
#     df = df.withColumn("filename", F.lit(file))
#     df = df.withColumn("annee", F.regexp_extract("filename", r"presi(\d{4})", 1))
    
#     # Colonnes clés à ne pas unpivoter
#     key_columns = {"Code département", "Code département0", "Code département1", "département", "circonscription",
#                    "Inscrits", "Votants", "Exprimés", "Blancs et nuls", "filename", "annee"}
    
#     # Détermination de la colonne à utiliser pour le code département
#     if "Code département" in df.columns:
#         dept_col = "Code département"
#     elif "Code département0" in df.columns:
#         dept_col = "Code département0"
#     else:
#         print(f"Le fichier {file} ne contient pas de colonne de code département attendue.")
#         continue

#     # Colonnes candidates (contenant les votes)
#     candidate_columns = [c for c in df.columns if c not in key_columns]
#     n = len(candidate_columns)
#     if n == 0:
#         continue

#     # Construction de l'expression STACK pour unpivot
#     expr_parts = []
#     for col in candidate_columns:
#         escaped_col = col.replace("'", "''")
#         expr_parts.append(f"'{escaped_col}', cast(`{col}` as int)")
#     expr = f"stack({n}, {', '.join(expr_parts)}) as (candidat, voix)"
    
#     # Sélection des colonnes : année, code département et unpivot
#     df_unpivot = df.select("annee", F.col(dept_col).alias("code_dept"), F.expr(expr))
    
#     # Agrégation des votes par département, par candidat et par année
#     df_agg = df_unpivot.groupBy("annee", "code_dept", "candidat").agg(F.sum("voix").alias("total_voix"))
    
#     # Sélection du candidat avec le maximum de voix par département et par année
#     windowSpec = Window.partitionBy("annee", "code_dept").orderBy(F.desc("total_voix"))
#     df_winner = df_agg.withColumn("rank", F.row_number().over(windowSpec)).filter(F.col("rank") == 1)
    
#     # Sélection des colonnes d'intérêt
#     results.append(df_winner.select("annee", "code_dept", F.col("candidat").alias("gagnant"), "total_voix"))

# # Union de tous les résultats (plusieurs années)
# if results:
#     final_df = results[0]
#     for df in results[1:]:
#         final_df = final_df.union(df)
    
#     # Ajout de la colonne avec le nom du département au bon format
#     final_df = final_df.withColumn(
#         "code_dept_norm",
#         F.when(F.col("code_dept").rlike("^[0-9]$"), F.lpad(F.col("code_dept"), 2, "0"))
#         .otherwise(F.col("code_dept"))
#     )

#     # Suppression de la colonne "code_dept" et renommage de la colonne "code_dept_norm" en "code_dept"
#     final_df = final_df.drop("code_dept").withColumnRenamed("code_dept_norm", "code_dept")

#     final_df.show(truncate=False)
    
#     # Réduction à une seule partition pour obtenir un unique fichier CSV
#     final_df.coalesce(1) \
#         .write \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .csv("president_per_department_with_dept_name")
    
#     print("Le CSV a été sauvegardé dans le dossier /mnt/data/president_per_department_with_dept_name")
# else:
#     print("Aucun fichier n'a été traité.")



# ------------------------------------------------------------------------------
# 2. Traitement du fichier 2017
# ------------------------------------------------------------------------------
# Lecture du fichier 2017 via Spark-Excel  
# On utilise sheetName (ou dataAddress si vous devez spécifier une plage)
df_2017_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Départements Tour 2'!A3:Z1000") \
    .load("./data/politique/taux-votes/2017/Presidentielle_2017_Resultats_Tour_2_c.xls")

print("=== Colonnes détectées pour 2017 ===")
print(df_2017_raw.columns)
df_2017_raw.show(5, truncate=False)

# Création des colonnes candidates en se basant sur la structure du fichier 2017  
# On suppose que pour chaque département, le premier candidat est défini par
# Nom17, Prénom18, Voix19 et le second par Nom23, Prénom24, Voix25.
df_2017 = df_2017_raw.withColumnRenamed("Code du département", "code_dept") \
    .withColumn("candidat1", F.concat(F.col("Nom17"), F.lit(" "), F.col("Prénom18"))) \
    .withColumn("candidat2", F.concat(F.col("Nom23"), F.lit(" "), F.col("Prénom24"))) \
    .select(F.col("code_dept").cast("string"), 
            F.col("Voix19").alias("voix1").cast("int"), 
            F.col("Voix25").alias("voix2").cast("int"),
            "candidat1", "candidat2")

# On crée un DataFrame par candidat
df_2017_candidate1 = df_2017.select("code_dept", 
                                    F.col("candidat1").alias("candidat"), 
                                    F.col("voix1").alias("voix"))
df_2017_candidate2 = df_2017.select("code_dept", 
                                    F.col("candidat2").alias("candidat"), 
                                    F.col("voix2").alias("voix"))
# Union des deux candidats
df_2017_norm = df_2017_candidate1.union(df_2017_candidate2) \
                    .withColumn("annee", F.lit("2017"))

# Pour chaque département, on garde le candidat avec le maximum de voix
w_dept = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
df_2017_final = df_2017_norm.withColumn("rank", F.row_number().over(w_dept)) \
                    .filter(F.col("rank") == 1) \
                    .select("annee", "code_dept", "candidat", "voix")

# ------------------------------------------------------------------------------
# 3. Traitement du fichier 2022
# ------------------------------------------------------------------------------
# Lecture du fichier 2022 via Spark-Excel  
df_2022_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sheetName", "Résultats") \
    .load("./data/politique/taux-votes/2022/resultats-par-niveau-subcom-t2-france-entiere.xlsx")

print("=== Colonnes détectées pour 2022 ===")
print(df_2022_raw.columns)
df_2022_raw.show(5, truncate=False)

# Pour 2022, on suppose que chaque ligne correspond déjà à un candidat,
# avec "Code du département", "Nom", "Prénom" et "Voix".
df_2022 = df_2022_raw.withColumnRenamed("Code du département", "code_dept") \
    .withColumn("candidat", F.concat(F.col("Nom"), F.lit(" "), F.col("Prénom"))) \
    .select(F.col("code_dept").cast("string"), "candidat", F.col("Voix").alias("voix")) \
    .withColumn("annee", F.lit("2022"))

# On agrège par département pour sélectionner le candidat gagnant (le plus de voix)
w_dept_2022 = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
df_2022_final = df_2022.withColumn("rank", F.row_number().over(w_dept_2022)) \
    .filter(F.col("rank") == 1) \
    .select("annee", "code_dept", "candidat", "voix")

# ------------------------------------------------------------------------------
# 4. Union des résultats et écriture du CSV final
# ------------------------------------------------------------------------------

# Ce dictionnaire unifie à la fois les codes "ZA", "ZB" etc. et les codes numériques
# (971, 972, etc.) pour avoir un seul libellé final (ex. "Guadeloupe").
# Ajoutez ou modifiez selon votre besoin.
dept_mapping = {
    # Guadeloupe
    "ZA": "Guadeloupe", "971": "Guadeloupe",
    # Martinique
    "ZB": "Martinique", "972": "Martinique",
    # Guyane
    "ZC": "Guyane", "973": "Guyane",
    # La Réunion
    "ZD": "La Réunion", "974": "La Réunion",
    # Mayotte
    "ZM": "Mayotte", "976": "Mayotte",
    # Nouvelle-Calédonie
    "ZN": "Nouvelle-Calédonie", "988": "Nouvelle-Calédonie",
    # Polynésie française
    "ZP": "Polynésie française", "987": "Polynésie française",
    # Saint-Pierre-et-Miquelon
    "ZS": "Saint-Pierre-et-Miquelon", "975": "Saint-Pierre-et-Miquelon",
    # Saint-Martin/Saint-Barthélemy
    "ZX": "Saint-Martin/Saint-Barthélemy",
    # Wallis et Futuna
    "ZW": "Wallis et Futuna", "986": "Wallis et Futuna",
    # Français établis hors de France
    "ZZ": "Français établis hors de France", "99": "Français établis hors de France",
    # Corse
    "2A": "Corse-du-Sud",
    "2B": "Haute-Corse"
    # Ajoutez d'autres mappings si besoin
}

df_final = df_2017_final.union(df_2022_final)

# 1) Convertir voix en int (si ce n'est pas déjà fait)
df_final = df_final.withColumn("voix", F.col("voix").cast("int"))

# ------------------------------------------------------------------------------
# 3. Créer une UDF pour remplacer code_dept par le libellé final
# ------------------------------------------------------------------------------
@F.udf(returnType=StringType())
def unify_dept_code(code):
    if code is None:
        return None
    # Supprimer la terminaison .0 s'il y en a (ex. "1.0" -> "1")
    code_str = code.replace(".0", "")
    # Normaliser un seul chiffre en 2 chiffres (ex. "1" -> "01")
    if code_str.isdigit() and len(code_str) == 1:
        code_str = f"0{code_str}"
    # Retourner le libellé final si présent dans dept_mapping
    return dept_mapping.get(code_str, code_str)

df_final = df_final.withColumn("code_dept", unify_dept_code(F.col("code_dept")))



# Vous pouvez ensuite afficher ou sauvegarder ce DataFrame
df_final.show(500,truncate=False)

# Écriture en un fichier CSV unique (le dossier contiendra un fichier part-0000*.csv)
# df_final.coalesce(1) \
#     .write \
#     .option("header", "true") \
#     .mode("overwrite") \
#     .csv("./data/politique/taux-votes/president_2017_2022_excel")

# print("Le CSV final a été sauvegardé dans ./data/politique/taux-votes/president_2017_2022_excel")

spark.stop()
