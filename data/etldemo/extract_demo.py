# extract.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
import os
from openpyxl import load_workbook  # Seulement pour lister les feuilles (sans pandas)
import xlrd  # Installer avec: pip install xlrd
def list_excel_sheets(excel_path: str) -> list:
    """
    Liste les noms de feuilles du fichier Excel (.xls).
    """
    wb = xlrd.open_workbook(excel_path, on_demand=True)
    sheets = wb.sheet_names()
    wb.release_resources()
    return sheets

def extract_data(spark: SparkSession, excel_path: str) -> DataFrame:
    """
    Extrait et fusionne les données des feuilles Excel (sauf la première) en un DataFrame Spark.
    Utilise spark-excel pour lire chaque feuille à partir de la 4ème ligne (A4).
    """
    sheets = list_excel_sheets(excel_path)
    # On ignore la première feuille (par exemple une feuille d'informations)
    sheets_to_read = sheets[1:]
    df_list = []
    for sheet in sheets_to_read:
        print(f"📄 Traitement de la feuille : {sheet}")
        dataAddress = f"'{sheet}'!A4"  # Lecture à partir de la 4ème ligne
        df_sheet = spark.read.format("com.crealytics.spark.excel") \
            .option("dataAddress", dataAddress) \
            .option("useHeader", "true") \
            .option("inferSchema", "true") \
            .option("treatEmptyValuesAsNulls", "true") \
            .option("addColorColumns", "false") \
            .load(excel_path)
        # Ajout de la colonne Année (issu du nom de la feuille)
        df_sheet = df_sheet.withColumn("Année", lit(sheet))
        df_list.append(df_sheet)
    if df_list:
        df_all = df_list[0]
        for df in df_list[1:]:
            df_all = df_all.unionByName(df)
        return df_all
    else:
        raise ValueError("Aucune feuille trouvée dans le fichier Excel.")
