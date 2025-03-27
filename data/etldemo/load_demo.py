# load.py
import os
import shutil
from pyspark.sql import DataFrame

def write_spark_output(df: DataFrame, output_folder: str):
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

def merge_spark_output(spark_output_dir: str, output_file: str):
    """
    Fusionne les fichiers CSV générés par Spark en un seul fichier CSV.
    """
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Définition du header à écrire
    headers = "Code_Département;Nom_Département;E_0_19_ans;E_20_39_ans;E_40_59_ans;E_60_74_ans;E_75_et_plus;E_Total;" \
              "F_0_19_ans;F_20_39_ans;F_40_59_ans;F_60_74_ans;F_75_et_plus;F_Total;" \
              "H_0_19_ans;H_20_39_ans;H_40_59_ans;H_60_74_ans;H_75_et_plus;H_Total;Année;Code_Département_Num;Type\n"
    
    with open(output_file, "w", encoding="utf-8") as outfile:
        outfile.write(headers)
        for part_file in sorted(os.listdir(spark_output_dir)):
            part_path = os.path.join(spark_output_dir, part_file)
            if part_file.startswith("part-") and part_file.endswith(".csv"):
                with open(part_path, "r", encoding="utf-8") as infile:
                    next(infile)  # Saut du header du fichier partiel
                    outfile.write(infile.read())
    shutil.rmtree(spark_output_dir)
    print(f"✓ Fichier fusionné créé : {output_file}")
