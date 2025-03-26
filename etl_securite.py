import pandas as pd
from collections import defaultdict

# Charger le fichier Excel
fichier_excel = "./data/securite/tableaux-4001-ts.xlsx"  # Mets le chemin complet si nécessaire
xls = pd.ExcelFile(fichier_excel)

# Liste des feuilles correspondant aux départements (on ignore France_Entière, France_Métro, etc.)
departements = [sheet for sheet in xls.sheet_names if sheet.isdigit()]

# Dictionnaire pour stocker les résultats {(département, année): total}
resultats = defaultdict(int)

# Plage d'années ciblée
annees_cibles = set(range(1996, 2023))  # de 1996 à 2022 inclus

# Traitement de chaque feuille
for dept in departements:
    try:
        df = xls.parse(dept)
        df = df.dropna(how="all")  # retirer les lignes totalement vides

        for col in df.columns:
            if isinstance(col, str) and col.startswith("_"):
                try:
                    annee = int(col.split("_")[1])
                    if annee in annees_cibles:
                        resultats[(dept, annee)] += df[col].sum(skipna=True)
                except:
                    continue
    except Exception as e:
        print(f"Erreur sur {dept} : {e}")
        continue

# Création du DataFrame final
df_final = pd.DataFrame([
    {"Département": dept, "Année": annee, "Délits_total": total}
    for (dept, annee), total in resultats.items()
])

# Tri pour lisibilité
df_final = df_final.sort_values(by=["Département", "Année"])

# Affichage
print(df_final)

# Export en CSV si besoin :
df_final.to_csv("delits_par_departement_1996_2022.csv", index=False)
