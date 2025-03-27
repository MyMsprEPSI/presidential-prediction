# transform.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType
from typing import Tuple

def transform_demographie_data(df: DataFrame) -> DataFrame:
    """
    Transformation initiale du DataFrame démographie.
    Renommage des colonnes, filtrage, ajout de colonne numérique pour tri et réorganisation.
    """
    df = df.withColumnRenamed("Départements", "Code_Département") \
        .withColumnRenamed("Unnamed: 1", "Nom_Département") \
        .withColumnRenamed("Ensemble", "E_Total") \
        .withColumnRenamed("Hommes", "H_Total") \
        .withColumnRenamed("Femmes", "F_Total")
    
    # Renommage pour les tranches d'âge – Ensemble (E)
    df = df.withColumnRenamed("Unnamed: 3", "E_0_19_ans") \
        .withColumnRenamed("Unnamed: 4", "E_20_39_ans") \
        .withColumnRenamed("Unnamed: 5", "E_40_59_ans") \
        .withColumnRenamed("Unnamed: 6", "E_60_74_ans") \
        .withColumnRenamed("Unnamed: 7", "E_75_et_plus")
    
    # Pour Hommes (H)
    df = df.withColumnRenamed("Unnamed: 9", "H_0_19_ans") \
        .withColumnRenamed("Unnamed: 10", "H_20_39_ans") \
        .withColumnRenamed("Unnamed: 11", "H_40_59_ans") \
        .withColumnRenamed("Unnamed: 12", "H_60_74_ans") \
        .withColumnRenamed("Unnamed: 13", "H_75_et_plus")
    
    # Pour Femmes (F)
    df = df.withColumnRenamed("Unnamed: 15", "F_0_19_ans") \
        .withColumnRenamed("Unnamed: 16", "F_20_39_ans") \
        .withColumnRenamed("Unnamed: 17", "F_40_59_ans") \
        .withColumnRenamed("Unnamed: 18", "F_60_74_ans") \
        .withColumnRenamed("Unnamed: 19", "F_75_et_plus")
    
    # Filtrage des lignes indésirables (notes, sources, etc.)
    df = df.filter(
        (~col("Code_Département").startswith("Source")) &
        (~col("Code_Département").startswith("NB:")) &
        (~col("Code_Département").contains("("))
    )
    
    # Ajout d'une colonne numérique pour le tri
    df = df.withColumn("Code_Département_Num", col("Code_Département").cast(IntegerType()))
    
    # Réorganisation des colonnes dans l'ordre souhaité
    cols_order = [
        "Code_Département", "Nom_Département",
        "E_0_19_ans", "E_20_39_ans", "E_40_59_ans", "E_60_74_ans", "E_75_et_plus", "E_Total",
        "F_0_19_ans", "F_20_39_ans", "F_40_59_ans", "F_60_74_ans", "F_75_et_plus", "F_Total",
        "H_0_19_ans", "H_20_39_ans", "H_40_59_ans", "H_60_74_ans", "H_75_et_plus", "H_Total",
        "Année", "Code_Département_Num"
    ]
    df = df.select(*cols_order)
    
    # Tri par Année décroissante puis par Code_Département_Num
    df = df.orderBy(col("Année").desc(), col("Code_Département_Num"))
    
    return df

def separate_totals(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Sépare les totaux (France Métropolitaine et DOM-TOM) des données départementales.
    """
    df_totaux = df.filter(
        col("Code_Département").contains("France") | col("Code_Département").contains("DOM")
    )
    df_departements = df.subtract(df_totaux)
    
    df_totaux = df_totaux.orderBy(col("Année").desc(), col("Code_Département_Num"))
    df_departements = df_departements.orderBy(col("Année").desc(), col("Code_Département_Num"))
    
    return df_totaux, df_departements

def final_cleaning_totaux(df_totaux: DataFrame) -> DataFrame:
    """
    Applique un nettoyage final sur le DataFrame des totaux en mettant à jour les codes et libellés.
    Ajoute également une colonne 'Type'.
    """
    df_totaux = df_totaux.withColumn("Type", 
                    when(col("Code_Département") == "France métropolitaine", "Métropole")
                    .when(col("Code_Département") == "France métropolitaine et DOM", "Métropole + DOM")
                    .otherwise("DOM"))
    
    df_totaux = df_totaux.withColumn("Nom_Département", 
                    when(col("Code_Département") == "France métropolitaine", "France métropolitaine")
                    .when(col("Code_Département") == "DOM", "Départements d'Outre-Mer")
                    .when(col("Code_Département") == "France métropolitaine et DOM", "France métropolitaine et DOM")
                    .otherwise(col("Nom_Département")))
    
    df_totaux = df_totaux.withColumn("Code_Département", 
                    when(col("Code_Département") == "France métropolitaine", "FRM")
                    .when(col("Code_Département") == "France métropolitaine et DOM", "FMD")
                    .when(col("Code_Département") == "DOM", "DOM")
                    .otherwise(col("Code_Département")))
    
    return df_totaux
