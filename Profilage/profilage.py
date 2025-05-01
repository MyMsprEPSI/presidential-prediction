import pandas as pd
import os
import sys
import io
from datetime import datetime

# Liste des fichiers à analyser
data_files = {
    "Environnemental": "./data/environnemental/parc-regional-annuel-prod-eolien-solaire.csv",
    "Demographie": "./data/demographie/estim-pop-dep-sexe-gca-1975-2023.xls",
    "Education": "./data/education/fr-en-etablissements-fermes.csv",
    "Santé": "./data/sante/valeurs_annuelles.csv",
    "Securité": "./data/securite/tableaux-4001-ts.xlsx",
    "technologie": "./data/technologie/technologie_pib_france_1990_2023.csv",
    "Socio-economique": "./data/socio-economie/PIB 1990 - 2021.xlsx",
    "Socio-economique_2022": "./data/socio-economie/PIB par Région en 2022.csv",
    "Politique": "./data/politique/vote_presidentiel_par_dept_1965_2022.csv",
}

# Fonction pour lire les fichiers selon leur type
def read_file(file_path, max_rows=5000):
    """
    Lit différents types de fichiers et retourne un DataFrame pandas
    Limite le nombre de lignes pour éviter les problèmes de mémoire
    """
    file_extension = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_extension in ['.xls', '.xlsx']:
            # Pour tous les fichiers Excel
            print(f"Lecture du fichier Excel: {file_path}")
            
            # Déterminer le moteur à utiliser
            engine = 'xlrd' if file_extension == '.xls' else 'openpyxl'
            
            # Obtenir la liste des feuilles
            xls = pd.ExcelFile(file_path, engine=engine)
            sheet_names = xls.sheet_names
            print(f"Feuilles disponibles: {sheet_names}")
            
            # Lire une feuille qui est susceptible de contenir des données
            # La première feuille peut souvent être une notice ou une introduction
            sheet_to_use = None
            
            # D'abord, essayer de trouver une feuille de données (souvent numérique)
            for sheet in sheet_names:
                if any(str(year) in sheet for year in range(1900, 2100)):
                    sheet_to_use = sheet
                    break
            
            # Si aucune feuille avec année trouvée, prendre la deuxième feuille si disponible
            if sheet_to_use is None and len(sheet_names) > 1:
                sheet_to_use = sheet_names[1]
            else:
                # Sinon, prendre la première
                sheet_to_use = sheet_names[0]
            
            print(f"Lecture de la feuille: {sheet_to_use}")
            
            # Lire seulement un sous-ensemble des données
            return pd.read_excel(file_path, engine=engine, sheet_name=sheet_to_use, nrows=max_rows)
                
        else:
            # Pour les fichiers CSV et autres formats
            print(f"Lecture du fichier CSV: {file_path}")
            return pd.read_csv(file_path, nrows=max_rows)
            
    except Exception as e:
        print(f"⚠️ Erreur lors de la lecture du fichier: {str(e)}")
        return None

# Fonction de profilage utilisant uniquement pandas
def profilage_pandas(df, nom_df):
    """
    Réalise le profilage d'un DataFrame pandas
    """
    if df is None or df.empty:
        print(f"\n--- Profilage : {nom_df} ---")
        print("⚠️ DataFrame vide ou invalide")
        return "⚠️ DataFrame vide ou invalide\n"
        
    try:
        result = []
        result.append(f"\n--- Profilage : {nom_df} ---")

        # Nombre total de lignes et colonnes
        result.append(f"Total lignes : {len(df)}")
        result.append(f"Total colonnes : {len(df.columns)}")

        # Afficher les informations sur les colonnes
        result.append("\nInformations sur les colonnes:")
        buffer = io.StringIO()
        df.info(buf=buffer)
        result.append(buffer.getvalue())
        
        # Aperçu des données
        result.append("\nAperçu des données:")
        result.append(df.head(5).to_string())

        # Valeurs manquantes
        result.append("\nValeurs manquantes par colonne:")
        missing = df.isnull().sum()
        missing_percent = (missing / len(df)) * 100
        missing_info = pd.DataFrame({
            'Nombre manquants': missing,
            'Pourcentage': missing_percent
        })
        result.append(missing_info.to_string())
        
        # Statistiques descriptives pour les colonnes numériques
        result.append("\nStatistiques descriptives (colonnes numériques):")
        numeric_df = df.select_dtypes(include=['number'])
        if not numeric_df.empty:
            result.append(df.describe().to_string())
        else:
            result.append("Aucune colonne numérique détectée")
        
        # Analyser la distribution des colonnes catégorielles
        result.append("\nValeurs uniques dans les colonnes catégorielles:")
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns
        
        for col in categorical_columns:
            unique_count = df[col].nunique()
            result.append(f"\nColonne: {col}")
            result.append(f"- Nombre de valeurs uniques: {unique_count}")
            
            # Si nombre de valeurs uniques est raisonnable, afficher les plus fréquentes
            if unique_count < 50:  # Limite arbitraire pour éviter des sorties trop longues
                value_counts = df[col].value_counts().head(10)  # Top 10 des valeurs
                result.append("- Valeurs les plus fréquentes:")
                result.append(value_counts.to_string())
        
        # Détection des valeurs extrêmes pour les colonnes numériques
        if not numeric_df.empty:
            result.append("\nValeurs extrêmes potentielles:")
            
            for col in numeric_df.columns:
                mean_val = df[col].mean()
                std_val = df[col].std()
                
                if pd.notna(mean_val) and pd.notna(std_val):
                    lower_bound = mean_val - 3 * std_val
                    upper_bound = mean_val + 3 * std_val
                    
                    # Compter les valeurs extrêmes
                    outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
                    outlier_count = len(outliers)
                    
                    if outlier_count > 0:
                        result.append(f"\nColonne: {col}")
                        result.append(f"- Moyenne: {mean_val:.2f}")
                        result.append(f"- Écart-type: {std_val:.2f}")
                        result.append(f"- Seuils: [{lower_bound:.2f}, {upper_bound:.2f}]")
                        result.append(f"- Nombre de valeurs extrêmes: {outlier_count} ({(outlier_count/len(df))*100:.2f}%)")
                        result.append("- Exemples de valeurs extrêmes:")
                        result.append(str(outliers.head(5).values))
            
        return "\n".join(result)
            
    except Exception as e:
        print(f"⚠️ Erreur durant le profilage: {str(e)}")
        return f"⚠️ Erreur durant le profilage: {str(e)}\n"

# Créer un fichier de résultats horodaté
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
results_file = f"./Profilage/resultats_profilage_{timestamp}.txt"

with open(results_file, "w", encoding="utf-8") as f:
    f.write("=== RAPPORT DE PROFILAGE DES DONNÉES ===\n")
    f.write(f"Date: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
    
    for nom_df, file_path in data_files.items():
        try:
            f.write(f"==== {nom_df} ====\n")
            f.write(f"Fichier: {file_path}\n")
            
            if os.path.exists(file_path):
                # Lire le fichier avec pandas
                df = read_file(file_path, max_rows=5000)
                
                # Profilage avec pandas
                if df is not None and not df.empty:
                    output = profilage_pandas(df, nom_df)
                    f.write(output)
                else:
                    f.write(f"⚠️ Impossible de lire le fichier ou DataFrame vide: {file_path}\n")
            else:
                f.write(f"⚠️ Fichier non trouvé: {file_path}\n")
        except Exception as e:
            f.write(f"⚠️ ERREUR lors du traitement du fichier {nom_df}: {str(e)}\n")
            print(f"Erreur lors du traitement du fichier {nom_df}: {e}")
        
        f.write("\n" + "="*50 + "\n\n")

print(f"\nRésultats sauvegardés dans: {results_file}")