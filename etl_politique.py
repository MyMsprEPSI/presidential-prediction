from pyspark.sql import SparkSession, functions as F , types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col ,expr
)
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

# Chargement de tous les fichiers CSV (les noms respectent le motif "cdsp_presi*t2_circ.csv")
file_list  = glob.glob("./data/politique/taux-votes/1965_2012/cdsp_presi*t2_circ.csv")

results = []

for file in file_list:
    # Lecture du fichier CSV avec en-tête et schéma inféré
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)

    # Ajout d'une colonne pour extraire l'année depuis le nom du fichier
    df = df.withColumn("filename", F.lit(file))
    df = df.withColumn("annee", F.regexp_extract("filename", r"presi(\d{4})", 1))

    # Colonnes clés à ne pas unpivoter
    key_columns = {"Code département", "Code département0", "Code département1", "département", "circonscription",
                   "Inscrits", "Votants", "Exprimés", "Blancs et nuls", "filename", "annee"}

    # Détermination de la colonne à utiliser pour le code département
    if "Code département" in df.columns:
        dept_col = "Code département"
    elif "Code département0" in df.columns:
        dept_col = "Code département0"
    else:
        print(f"Le fichier {file} ne contient pas de colonne de code département attendue.")
        continue

    # Colonnes candidates (contenant les votes)
    candidate_columns = [c for c in df.columns if c not in key_columns]
    n = len(candidate_columns)
    if n == 0:
        continue

    # Construction de l'expression STACK pour unpivot
    expr_parts = []
    for col in candidate_columns:
        escaped_col = col.replace("'", "''")
        expr_parts.append(f"'{escaped_col}', cast(`{col}` as int)")
    expr = f"stack({n}, {', '.join(expr_parts)}) as (candidat, voix)"

    # Sélection des colonnes : année, code département et unpivot
    df_unpivot = df.select("annee", F.col(dept_col).alias("code_dept"), F.expr(expr))

    # Agrégation des votes par département, par candidat et par année
    df_agg = df_unpivot.groupBy("annee", "code_dept", "candidat").agg(F.sum("voix").alias("total_voix"))

    # Sélection du candidat avec le maximum de voix par département et par année
    windowSpec = Window.partitionBy("annee", "code_dept").orderBy(F.desc("total_voix"))
    df_winner = df_agg.withColumn("rank", F.row_number().over(windowSpec)).filter(F.col("rank") == 1)

    # Option 1 : créer une colonne "gagnant" à partir de "candidat"
    df_winner = df_winner.withColumn("gagnant", F.col("candidat"))

    # Ensuite, on enlève la partie entre parenthèses
    df_winner = df_winner.withColumn(
        "gagnant",
        F.trim(F.regexp_replace(F.col("gagnant"), r"\([^)]*\)", ""))
    )

    # Puis on applique le mapping pour obtenir le format "Prénom Nom"
    df_winner = df_winner.withColumn(
        "gagnant",
        F.when(F.col("gagnant") == "SARKOZY", "Nicolas SARKOZY")
        .when(F.col("gagnant") == "CHIRAC", "Jacques CHIRAC")
        .when(F.col("gagnant") == "MITTERRAND", "François MITTERRAND")
        .when(F.col("gagnant") == "DE GAULLE", "Charles DE GAULLE")
        .when(F.col("gagnant") == "GISCARD DESTAING", "Valéry GISCARD DESTAING")
        .when(F.col("gagnant") == "POMPIDOU", "Georges POMPIDOU")
        .when(F.col("gagnant") == "POHER", "Alain POHER")
        .when(F.col("gagnant") == "JOSPIN", "Lionel JOSPIN")
        .when(F.col("gagnant") == "ROYAL", "Ségolène ROYAL")
        .when(F.col("gagnant") == "HOLLANDE", "François HOLLANDE")
        .when(F.col("gagnant") == "MACRON", "Emmanuel MACRON")
        .when(F.col("gagnant") == "LE PEN", "Marine LE PEN")
        .otherwise(F.col("gagnant"))
    )

    # Sélection des colonnes d'intérêt
    results.append(df_winner.select("annee", "code_dept", F.col("gagnant").alias("candidat"), "total_voix"))


# Union de tous les résultats (plusieurs années)
if results:
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.union(df)

    # Ajout de la colonne avec le nom du département au bon format
    final_df = final_df.withColumn(
        "code_dept_norm",
        F.when(F.col("code_dept").rlike("^[0-9]$"), F.lpad(F.col("code_dept"), 2, "0"))
        .otherwise(F.col("code_dept"))
    )

    # Suppression de la colonne "code_dept" et renommage de la colonne "code_dept_norm" en "code_dept"
    final_df = final_df.drop("code_dept").withColumnRenamed("code_dept_norm", "code_dept")
    
    # print("Le CSV a été sauvegardé dans le dossier /mnt/data/president_per_department_with_dept_name")
else:
    print("Aucun fichier n'a été traité.")



# ------------------------------------------------------------------------------
# 2. Traitement du fichier 2017
# ------------------------------------------------------------------------------
# Lecture du fichier 2017 via Spark-Excel  
# On utilise sheetName (ou dataAddress si vous devez spécifier une plage)
df_2017_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Départements Tour 2'!A3:Z115") \
    .load("./data/politique/taux-votes/2017/Presidentielle_2017_Resultats_Tour_2_c.xls")

print("=== Colonnes détectées pour 2017 ===")

# Création des colonnes candidates en se basant sur la structure du fichier 2017  
# On suppose que pour chaque département, le premier candidat est défini par
# Nom17, Prénom18, Voix19 et le second par Nom23, Prénom24, Voix25.
# On crée donc deux colonnes candidat1 et candidat2 et le libellé du département
df_2017 = df_2017_raw.withColumnRenamed("Code du département", "code_dept") \
    .withColumn("candidat1", F.concat(F.col("Nom17"), F.lit(" "), F.col("Prénom18"))) \
    .withColumn("candidat2", F.concat(F.col("Nom23"), F.lit(" "), F.col("Prénom24"))) \
    .select(F.col("code_dept").cast("string"),
            F.col("Libellé du département"),
            F.col("Voix19").alias("voix1").cast("int"), 
            F.col("Voix25").alias("voix2").cast("int"),
            "candidat1", "candidat2"
    )


# On crée un DataFrame par candidat
df_2017_candidate1 = df_2017.select("code_dept", 
                                    F.col("candidat1").alias("candidat"), 
                                    F.col("voix1").alias("voix"),
                                    F.col("Libellé du département")
                                    )


df_2017_candidate2 = df_2017.select("code_dept", 
                                    F.col("candidat2").alias("candidat"), 
                                    F.col("voix2").alias("voix"),
                                    F.col("Libellé du département")
                                    )


# Union des deux candidats
df_2017_norm = df_2017_candidate1.union(df_2017_candidate2) \
                    .withColumn("annee", F.lit("2017"))


# 1. Appliquer le mapping pour les codes spéciaux et la Corse
df_2017_norm = df_2017_norm.withColumn(
    "code_dept_norm",
    F.when(F.col("Libellé du département") == "Guadeloupe", "ZA")
     .when(F.col("Libellé du département") == "Martinique", "ZB")
     .when(F.col("Libellé du département") == "Guyane", "ZC")
     .when(F.col("Libellé du département") == "La Réunion", "ZD")
     .when(F.col("Libellé du département") == "Mayotte", "ZM")
     .when(F.col("Libellé du département") == "Nouvelle-Calédonie", "ZN")
     .when(F.col("Libellé du département") == "Polynésie française", "ZP")
     .when(F.col("Libellé du département") == "Saint-Pierre-et-Miquelon", "ZS")
     .when(F.col("Libellé du département") == "Saint-Martin/Saint-Barthélemy", "ZX")
     .when(F.col("Libellé du département") == "Wallis et Futuna", "ZW")
     .when(F.col("Libellé du département") == "Français établis hors de France", "ZZ")
     .when(F.col("Libellé du département") == "Corse-du-Sud", "2A")
     .when(F.col("Libellé du département") == "Haute-Corse", "2B")
     .otherwise(F.col("code_dept"))
)

# 1. Supprimer la terminaison ".0" dans la colonne "code_dept_norm"
df_final_2017 = df_2017_norm.withColumn(
    "code_dept_final",
    F.regexp_replace(F.col("code_dept_norm"), r"\.0$", "")
)

# 2. (Optionnel) Si vous souhaitez que les codes sur un seul chiffre soient affichés sur 2 chiffres (ex. "1" -> "01")
df_final_2017 = df_final_2017.withColumn(
    "code_dept_final",
    F.when(F.col("code_dept_final").rlike("^[0-9]$"),
           F.lpad(F.col("code_dept_final"), 2, "0"))
     .otherwise(F.col("code_dept_final"))
)

# 3. Supprimer les colonnes intermédiaires et renommer la colonne finale en "code_dept"
df_final_2017 = df_final_2017.drop("code_dept", "code_dept_norm", "Libellé du département") \
                   .withColumnRenamed("code_dept_final", "code_dept")



# Pour chaque département, on garde le candidat avec le maximum de voix
w_dept = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
df_2017_final = df_final_2017.withColumn("rank", F.row_number().over(w_dept)) \
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



df_final = df_2017_final.union(df_2022_final)

# 1. Appliquer le mapping pour les codes spéciaux
df_final = df_final.withColumn(
    "code_dept",
    F.when(F.col("code_dept") == "ZA", "971")
     .when(F.col("code_dept") == "ZB", "972")
     .when(F.col("code_dept") == "ZC", "973")
     .when(F.col("code_dept") == "ZD", "974")
     .when(F.col("code_dept") == "ZM", "976")
     .when(F.col("code_dept") == "ZN", "988")
     .when(F.col("code_dept") == "ZP", "987")
     .when(F.col("code_dept") == "ZS", "975")
     .when(F.col("code_dept") == "ZX", "971")
     .when(F.col("code_dept") == "ZW", "986")
     .when(F.col("code_dept") == "ZZ", "99")
     .otherwise(F.col("code_dept"))
)

# 1.1 Normalisation des candidats avec prénom et nom
df_final = df_final.withColumn(
    "candidat", 
    F.when(F.col("candidat") == "MACRON Emmanuel", "Emmanuel MACRON")
    .when(F.col("candidat") == "LE PEN Marine", "Marine LE PEN")
)

# 2. Appliquer le format int pour les voix
df_final = df_final.withColumn("voix", F.col("voix").cast("int"))

# 3. Renommer la colonne "voix" en "total_voix"
df_final = df_final.withColumnRenamed("voix", "total_voix")

# 4. Sélection des colonnes d'intérêt
final_df = final_df.select("annee", "code_dept", "candidat", "total_voix")

# 5. Union des deux DataFrames
df_final_csv = df_final.union(final_df)

# Affichage des données finales
df_final_csv.show(500,truncate=False)

# Ecriture de toutes les données dans un fichier CSV avec un seul fichier de sortie Pandas
df_final_csv.toPandas().to_csv("./data/politique/vote_presidentiel_par_dept_1965_2022.csv", index=False, header=True)

# Fermeture de la session Spark
spark.stop()
