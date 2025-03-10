from pyspark.sql import SparkSession, functions as F , types as T
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    udf , col , regexp_extract , lit , when , expr,
    concat_ws, concat , split , regexp_replace , lower,
    trim , upper , coalesce, collect_list
)
import pandas as pd
import re
import glob
import sys

# Création de la session Spark
spark = SparkSession.builder.appName("TechnologieSummary") \
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
                            

# Lecture du fichier Excel depuis la feuille "Tableau 1"
df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Tableau 1'!A3:B37") \
    .load("./data/technologie/Effort-recherche_tableaux_2024.xlsx")

# Afficher le schéma et quelques lignes pour vérifier les colonnes lues
df.printSchema()
df.show(5, truncate=False)

# 4. Sélection des colonnes Année et DIRD/PIB France (adaptez si vos en-têtes diffèrent)
df_selected = df.select(
    F.col("_c0").alias("annee").cast("string"),
    F.col("DIRD/PIB  France").alias("dird_pib_france_pourcentages").cast("float")
)

# Arrondir les valeurs de la colonne DIRD/PIB France à 2 décimales
df_selected = df_selected.withColumn("dird_pib_france_pourcentages", F.round(F.col("dird_pib_france_pourcentages"), 2))

# Supprimer les .0 à la fin des années
df_selected = df_selected.withColumn("annee", regexp_replace("annee", "\.0", ""))

# La valeur NaN dans année est remplacée par la valuer 2023
df_selected = df_selected.withColumn("annee", when(df_selected.annee == "NaN", "2023").otherwise(df_selected.annee))

# Afficher le schéma et quelques lignes pour vérifier les colonnes sélectionnées
df_selected.show(100, truncate=False)

# 5. Écriture du fichier CSV
df_selected.toPandas().to_csv("./data/technologie/technologie_pib_france_1990_2023.csv", index=False, header=True)

print("Fichier CSV écrit avec succès : ./data/technologie/technologie_pib_france_1990_2023.csv") 

# Stopper la session Spark
spark.stop()