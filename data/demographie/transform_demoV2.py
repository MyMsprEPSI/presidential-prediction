import os
import shutil
import subprocess
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, lit, year
from pyspark.sql.types import IntegerType

# ========================
# 1. Conversion XLS -> XLSX
# ========================
def convert_excel_to_xlsx(file_xls, file_xlsx):
    """
    Convertit le fichier XLS en XLSX si ce dernier n'existe pas déjà.
    """
    if not os.path.exists(file_xlsx):
        print("📥 Conversion du fichier XLS en XLSX...")
        try:
            # Charger toutes les feuilles du fichier XLS
            excel_data = pd.read_excel(file_xls, sheet_name=None)
            with pd.ExcelWriter(file_xlsx, engine="openpyxl") as writer:
                for sheet, data in excel_data.items():
                    data.to_excel(writer, sheet_name=sheet, index=False)
            print(f"✅ Conversion réussie : {file_xlsx}")
        except Exception as e:
            print(f"❌ Erreur lors de la conversion : {e}")
    else:
        print(f"✓ {file_xlsx} existe déjà. Conversion ignorée.")

# ================================
# 2. Extraction et fusion des données Excel en CSV
# ================================
def extract_and_merge_excel(file_xlsx, file_csv):
    """
    Extrait et fusionne les feuilles Excel (hors première feuille) en un unique CSV.
    """
    if not os.path.exists(file_csv):
        print("📥 Extraction et fusion des feuilles Excel...")
        try:
            # Charger le classeur XLSX
            xls = pd.ExcelFile(file_xlsx)
            # On ignore la première feuille (ex: "À savoir")
            sheets_to_read = xls.sheet_names[1:]
            dfs = []
            for sheet in sheets_to_read:
                print(f"📄 Traitement de la feuille : {sheet}")
                df = pd.read_excel(xls, sheet_name=sheet, skiprows=3)
                df["Année"] = sheet  # Ajouter l'année issue du nom de la feuille
                dfs.append(df)
            df_final = pd.concat(dfs, ignore_index=True)
            # Sauvegarder en CSV avec le séparateur point-virgule
            df_final.to_csv(file_csv, index=False, sep=";")
            print(f"✅ Fichier CSV généré : {file_csv}")
        except Exception as e:
            print(f"❌ Erreur lors de l'extraction/fusion : {e}")
    else:
        print(f"✓ {file_csv} existe déjà. Extraction ignorée.")

# ========================
# 3. Traitement avec PySpark
# ========================
def transform_demographie_data(df):
    """
    Effectue le renommage des colonnes, filtre et réorganise les données démographie.
    """
    # Renommage initial des colonnes principales
    df = df.withColumnRenamed("Départements", "Code_Département") \
           .withColumnRenamed("Unnamed: 1", "Nom_Département") \
           .withColumnRenamed("Ensemble", "E_Total") \
           .withColumnRenamed("Hommes", "H_Total") \
           .withColumnRenamed("Femmes", "F_Total")
    
    # Renommage des colonnes pour les tranches d'âge
    # Pour Ensemble (E)
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
    
    # Suppression des lignes de source/notes
    df = df.filter(
        (~col("Code_Département").startswith("Source")) &
        (~col("Code_Département").startswith("NB:")) &
        (~col("Code_Département").contains("("))
    )
    
    # Ajout d'une colonne numérique pour le tri uniquement
    df = df.withColumn("Code_Département_Num",
                      col("Code_Département").cast(IntegerType()))
    
    # Réorganisation des colonnes dans l'ordre souhaité
    df = df.select(
        "Code_Département", "Nom_Département",
        "E_0_19_ans", "E_20_39_ans", "E_40_59_ans", "E_60_74_ans", "E_75_et_plus", "E_Total",
        "F_0_19_ans", "F_20_39_ans", "F_40_59_ans", "F_60_74_ans", "F_75_et_plus", "F_Total",
        "H_0_19_ans", "H_20_39_ans", "H_40_59_ans", "H_60_74_ans", "H_75_et_plus", "H_Total",
        "Année", "Code_Département_Num"
    )
    
    # Tri par année décroissante puis par code département
    df = df.orderBy(col("Année").desc(), "Code_Département_Num")
    
    return df

def separate_totals(df):
    """
    Sépare les totaux (France Métropolitaine et DOM-TOM) des données départementales.
    """
    df_totaux = df.filter(
        col("Code_Département").contains("France") | col("Code_Département").contains("DOM")
    )
    df_departements = df.subtract(df_totaux)
    # Réordonner chacun
    df_totaux = df_totaux.orderBy(col("Année").desc(), "Code_Département_Num")
    df_departements = df_departements.orderBy(col("Année").desc(), "Code_Département_Num")
    return df_totaux, df_departements

# ========================
# 4. Écriture des sorties Spark et fusion
# ========================
def write_spark_output(df, output_folder):
    """
    Écrit le DataFrame Spark dans un dossier en forçant la sortie en un seul fichier CSV.
    """
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    try:
        (df.coalesce(1)
           .write
           .mode("overwrite")
           .option("header", True)
           .option("sep", ";")
           .csv(output_folder))
        print(f"✓ Données écrites dans {output_folder}")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture dans {output_folder} : {e}")

def merge_spark_output(spark_output_dir, output_file):
    """
    Fusionne les fichiers CSV générés par Spark en un seul fichier CSV.
    """
    # Si le fichier de sortie existe déjà, on le supprime
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Le header à écrire est fixé selon l'ordre des colonnes
    headers = "Code_Département;Nom_Département;E_0_19_ans;E_20_39_ans;E_40_59_ans;E_60_74_ans;E_75_et_plus;E_Total;" \
              "F_0_19_ans;F_20_39_ans;F_40_59_ans;F_60_74_ans;F_75_et_plus;F_Total;" \
              "H_0_19_ans;H_20_39_ans;H_40_59_ans;H_60_74_ans;H_75_et_plus;H_Total;Année;Code_Département_Num\n"
    
    with open(output_file, "w", encoding="utf-8") as outfile:
        outfile.write(headers)
        for part_file in sorted(os.listdir(spark_output_dir)):
            part_path = os.path.join(spark_output_dir, part_file)
            if part_file.startswith("part-") and part_file.endswith(".csv"):
                with open(part_path, "r", encoding="utf-8") as infile:
                    next(infile)  # Sauter le header du fichier partiel
                    outfile.write(infile.read())
    # Supprimer le dossier intermédiaire
    shutil.rmtree(spark_output_dir)
    print(f"✓ Fichier fusionné créé : {output_file}")

# ========================
# 5. Nettoyage final avec Pandas
# ========================
def final_cleaning(file_totaux, file_departements):
    """
    Effectue un dernier nettoyage sur le CSV des totaux en appliquant une correspondance pour les codes et libellés.
    Puis, réécrit les fichiers finaux.
    """
    try:
        # Chargement des CSV en DataFrame Pandas
        df_totaux_pd = pd.read_csv(file_totaux, sep=";")
        df_departements_pd = pd.read_csv(file_departements, sep=";")
        
        # Dictionnaire de correspondance
        code_mapping = {
            "France métropolitaine": {"code": "FRM", "nom": "France métropolitaine"},
            "DOM": {"code": "DOM", "nom": "Départements d'Outre-Mer"},
            "France métropolitaine et DOM": {"code": "FMD", "nom": "France métropolitaine et DOM"}
        }
        
        # Nettoyage des espaces et ajout de la colonne Type
        df_totaux_pd["Code_Département"] = df_totaux_pd["Code_Département"].str.strip()
        df_totaux_pd.insert(0, "Type", df_totaux_pd["Code_Département"].apply(
            lambda x: "Métropole" if x == "France métropolitaine" 
            else "Métropole + DOM" if x == "France métropolitaine et DOM" 
            else "DOM"))
        
        # Mise à jour des codes et noms avec le mapping
        df_totaux_pd["Nom_Département"] = df_totaux_pd["Code_Département"].map(lambda x: code_mapping.get(x, {}).get("nom", x))
        df_totaux_pd["Code_Département"] = df_totaux_pd["Code_Département"].map(lambda x: code_mapping.get(x, {}).get("code", x))
        
        # Réécriture des fichiers CSV finaux
        df_totaux_pd.to_csv(file_totaux, sep=";", index=False)
        df_departements_pd.to_csv(file_departements, sep=";", index=False)
        print("✅ Fichiers finaux écrits avec succès.")
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage final : {e}")

# ========================
# Fonction principale
# ========================
def main():
    # Configuration de Java et Spark
    os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'
    os.environ['SPARK_HOME'] = r'C:\Users\thoma\AppData\Roaming\Python\Python312\site-packages\pyspark'
    os.environ['PATH'] = os.environ['JAVA_HOME'] + r'\bin;' + os.environ['SPARK_HOME'] + r'\bin;' + os.environ['PATH']
    os.environ['PYSPARK_PYTHON'] = r'C:\Python312\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Python312\python.exe'
    
    # Vérification de Java
    try:
        subprocess.run(['java', '-version'], capture_output=True, check=True)
    except FileNotFoundError:
        print("Erreur: Java n'est pas trouvé. Veuillez installer Java 17 et configurer JAVA_HOME.")
        return

    # Chemins des fichiers
    file_xls = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/estim-pop-dep-sexe-gca-1975-2023.xls"
    file_xlsx = file_xls.replace(".xls", ".xlsx")
    file_csv = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_par_departement_annee.csv"
    
    # Conversion et extraction/fusion des données Excel
    convert_excel_to_xlsx(file_xls, file_xlsx)
    extract_and_merge_excel(file_xlsx, file_csv)
    
    # Lancement de Spark
    print("🚀 Lancement du traitement PySpark...")
    spark = SparkSession.builder \
            .appName("Traitement_Demographie") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .master("local[*]") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Lecture du CSV fusionné
    df_spark = spark.read.option("header", True).option("sep", ";").csv(file_csv)
    
    # Transformation des données démographie
    df_spark = transform_demographie_data(df_spark)
    
    # Séparation des totaux et des départements
    df_totaux, df_departements = separate_totals(df_spark)
    
    # Définition des dossiers et fichiers de sortie
    dir_departements = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_par_departement_spark"
    dir_totaux = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_totaux_spark"
    file_departements = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_par_departement_spark.csv"
    file_totaux = "D:/Thomas/Documents/GitHub/bigdata-project/data/demographie/population_totaux_spark.csv"
    
    # Écriture des sorties Spark
    write_spark_output(df_departements, dir_departements)
    write_spark_output(df_totaux, dir_totaux)
    
    # Fusion des sorties en un seul fichier CSV par ensemble
    merge_spark_output(dir_departements, file_departements)
    merge_spark_output(dir_totaux, file_totaux)
    
    # Nettoyage final avec Pandas
    final_cleaning(file_totaux, file_departements)
    
    # Arrêt de la session Spark
    spark.stop()
    print("🚀 Traitement terminé.")

if __name__ == "__main__":
    main()
