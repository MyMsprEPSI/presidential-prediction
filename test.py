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






def process_election_df(df, year, dept_col_candidates):
    """
    Traite un DataFrame d'élection en :
      - ajoutant la colonne 'annee' avec la valeur year
      - renommant la colonne de département sélectionnée en 'code_dept'
      - unpivotant les colonnes de votes (toutes les colonnes non clés)
      - agrégant les voix par (annee, code_dept, candidat)
      - sélectionnant le candidat avec le maximum de voix par département (pour l'année)
    
    dept_col_candidates : liste de noms possibles pour la colonne département.
    """
    # Ajout de la colonne annee
    df = df.withColumn("annee", F.lit(year))
    
    # Déterminer la colonne à utiliser pour le code département
    dept_col = None
    for col in dept_col_candidates:
        if col in df.columns:
            dept_col = col
            break
    if not dept_col:
        print(f"Le DataFrame pour l'année {year} ne contient pas de colonne de département parmi {dept_col_candidates}")
        return None
    
    # Renommage en 'code_dept' pour uniformiser
    df = df.withColumnRenamed(dept_col, "code_dept")
    
    # Définir les colonnes clés que l'on ne souhaite pas unpivoter
    key_columns = {"code_dept", "annee", "Inscrits", "Votants", "Exprimés", "Blancs et nuls"}
    candidate_columns = [c for c in df.columns if c not in key_columns]
    if len(candidate_columns) == 0:
        print(f"Aucune colonne candidat trouvée pour l'année {year}.")
        return None

    # Construction de l'expression STACK pour unpivot
    expr_parts = []
    for col in candidate_columns:
        # Chaque colonne candidate sera traitée comme vote (cast en int)
        expr_parts.append(f"'{col}', cast(`{col}` as int)")
    stack_expr = f"stack({len(candidate_columns)}, {', '.join(expr_parts)}) as (candidat, voix)"
    
    # Unpivot : on conserve annee, code_dept et on applique stack pour obtenir candidat et voix
    df_unpivot = df.select("annee", "code_dept", F.expr(stack_expr))
    
    # Agrégation des voix par département, candidat et année
    df_agg = df_unpivot.groupBy("annee", "code_dept", "candidat") \
                       .agg(F.sum("voix").alias("total_voix"))
    
    # Sélection du gagnant pour chaque (annee, code_dept)
    windowSpec = Window.partitionBy("annee", "code_dept").orderBy(F.desc("total_voix"))
    df_winner = df_agg.withColumn("rank", F.row_number().over(windowSpec)) \
                      .filter(F.col("rank") == 1)
    
    return df_winner.select("annee", "code_dept", F.col("candidat").alias("gagnant"), "total_voix")

# Liste des noms de colonnes pouvant contenir le code département (selon vos fichiers Excel)
dept_col_candidates = ["Code département", "Département"]

# Lecture du fichier 2017 (format .xls) via spark-excel
df_2017 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Circo. Leg. Tour 2'!A1") \
    .load("./data/politique/taux-votes/2017/Presidentielle_2017_Resultats_Tour_2_c.xls")

# Lecture du fichier 2022 (format .xlsx) via spark-excel
df_2022 = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Résultats par niveau BurVot T2'!A1") \
    .load("./data/politique/taux-votes/2022/resultats-par-niveau-burvot-t2-france-entiere.xlsx")

# Traitement de chaque DataFrame
df_2017_processed = process_election_df(df_2017, "2017", dept_col_candidates)
df_2022_processed = process_election_df(df_2022, "2022", dept_col_candidates)

if df_2017_processed is None or df_2022_processed is None:
    print("Erreur lors du traitement de l'un des fichiers.")
    sys.exit(1)

# Union des résultats des deux années
df_final = df_2017_processed.union(df_2022_processed)
df_final.show(truncate=False)

# Sauvegarde du résultat dans un unique fichier CSV (dans un dossier)
df_final.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("/mnt/data/president_2017_2022_excel")

print("Le CSV final a été sauvegardé dans le dossier /mnt/data/president_2017_2022_excel")







spark.stop()

