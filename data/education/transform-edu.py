import os
import subprocess
import pandas as pd

def main():
    # Configuration de Java (facultatif pour Pandas, mais conservé pour correspondre à l'original)
    os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'
    os.environ['PATH'] = os.environ['JAVA_HOME'] + r'\bin;' + os.environ['PATH']
    
    # Vérification de Java
    try:
        subprocess.run(['java', '-version'], capture_output=True, check=True)
    except FileNotFoundError:
        print("Erreur: Java n'est pas trouvé. Veuillez installer Java 17 et configurer JAVA_HOME.")
        return

    input_path = "data/education/fr-en-etablissements-fermes.csv"  # Chemin vers le CSV source
    output_folder = "data/education/output"              # Dossier de sortie pour le CSV transformé

    # Extraction des données
    df = extract_data(input_path)
    
    # Profilage des données
    profile_data(df)
    
    # Transformation et nettoyage des données
    df_transformed = transform_data(df)

    # Calcul du nombre d'établissements fermés par année et par département
    df_grouped = calculate_closed_by_year_and_dept(df_transformed)
    
    # Vous pouvez sauvegarder ce DataFrame agrégé dans un fichier CSV si besoin
    load_data(df_grouped, "data/education/output_grouped")
    
    # Chargement des données transformées
    load_data(df_transformed, output_folder)

def extract_data(input_path: str) -> pd.DataFrame:
    """
    Extrait les données depuis un fichier CSV.
    
    :param input_path: Chemin vers le CSV source
    :return: DataFrame contenant les données extraites
    """
    # Vérification de l'existence du fichier
    if not os.path.exists(input_path):
        print(f"Erreur: Le fichier {input_path} n'existe pas.")
        return pd.DataFrame()
    
    try:
        # D'abord, on lit les premières lignes pour vérifier le format
        with open(input_path, 'r', encoding='utf-8') as f:
            header = f.readline().strip().split(';')
            print(f"✓ En-têtes détectés: {', '.join(header)}")
            print(f"✓ Nombre de colonnes attendu: {len(header)}")
            
            # Vérification des premières lignes
            for i, line in enumerate(f, 2):
                if i <= 5:  # On vérifie les 5 premières lignes
                    fields = line.strip().split(';')
                    if len(fields) != len(header):
                        print(f"⚠️ Attention: Incohérence dans le nombre de colonnes à la ligne {i}")
                        print(f"  Colonnes attendues: {len(header)}, Colonnes trouvées: {len(fields)}")
                        print(f"  Ligne problématique: {line.strip()}")
        
        # Chargement du CSV avec gestion des erreurs
        df = pd.read_csv(input_path, 
                        header=0,
                        encoding='utf-8',
                        sep=';',
                        on_bad_lines='warn')  # Affiche un avertissement pour les lignes problématiques
        
        print(f"✓ Données chargées avec succès depuis {input_path}")
        print(f"✓ Nombre de lignes: {len(df)}")
        print(f"✓ Colonnes présentes: {', '.join(df.columns)}")
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement du fichier CSV : {str(e)}")
        print("Vérifiez que le fichier est bien formaté et que toutes les lignes ont le même nombre de colonnes.")
        return pd.DataFrame()
    
    return df

def profile_data(df: pd.DataFrame):
    """
    Effectue un profilage détaillé du DataFrame et propose des suggestions de conversion.
    """
    print("\n=== Profilage du DataFrame ===\n")
    
    # 1. Affichage du schéma inféré
    print("Schéma du DataFrame :")
    print(df.dtypes)
    
    # 2. Aperçu des premières lignes
    print("\nAperçu des 5 premières lignes :")
    print(df.head(5))
    
    # 3. Statistiques descriptives
    print("\nStatistiques descriptives :")
    print(df.describe(include='all'))
    
    # 4. Nombre total de lignes
    total_rows = len(df)
    print(f"\nNombre total de lignes : {total_rows}")
    
    # 5. Analyse des valeurs manquantes pour chaque colonne
    print("\n--- Analyse des valeurs manquantes ---")
    for column in df.columns:
        missing = df[column].isna().sum()
        missing_percent = (missing / total_rows) * 100
        print(f"Colonne '{column}': {missing} valeurs manquantes ({missing_percent:.2f}%)")
    
    # 6. Distribution pour les colonnes catégorielles (exemple: "etat")
    if "etat" in df.columns:
        print("\nDistribution des valeurs pour la colonne 'etat' :")
        print(df["etat"].value_counts().head(10))
    
    # 7. Suggestions de conversion et nettoyage
    print("\n=== Suggestions de conversion et nettoyage ===")
    
    # Exemples de suggestions basées sur un schéma courant d'établissements :
    if "date_fermeture" in df.columns:
        print("- 'date_fermeture' : Convertir en type Date avec pd.to_datetime().")
        
    if "code_postal" in df.columns:
        print("- 'code_postal' : Conserver en String pour préserver les zéros initiaux.")
        
    # Pour les colonnes textuelles, uniformiser la casse et nettoyer les espaces
    text_columns = df.select_dtypes(include=['object']).columns
    if len(text_columns) > 0:
        print(f"- Colonnes textuelles {list(text_columns)} : Appliquer str.strip() et str.lower() pour uniformiser.")
    
    print("- Vérifier et convertir les colonnes numériques au besoin.")
    
    print("\nCe profilage vous permettra d'adapter ensuite les transformations en fonction des spécificités de votre jeu de données.")


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme et nettoie les données.
    
    :param df: DataFrame issu de l'extraction
    :return: DataFrame transformé et nettoyé
    """
    print("\n=== TRANSFORMATION ET NETTOYAGE DES DONNÉES ===")
    
    # 1. Suppression des doublons
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f"Suppression des doublons: {initial_count - len(df)} lignes supprimées")
    
    # 2. Standardisation des colonnes textuelles (pour 'nom', 'adresse', 'ville', 'etat', etc.)
    text_columns = df.select_dtypes(include='object').columns
    for column in text_columns:
        # Appliquer trim et conversion en minuscules
        df[column] = df[column].astype(str).str.strip().str.lower()
    
    # 3. Gestion des valeurs manquantes pour les colonnes textuelles
    for column in text_columns:
        # Remplacer les chaînes vides ou "nan" par "non spécifié"
        df[column] = df[column].replace("nan", "").replace("", None)
        df[column] = df[column].fillna("non spécifié")
    
    # 4. Conversion de la colonne 'date_fermeture' en format date et extraction de l'année
    if "date_fermeture" in df.columns:
        df["date_fermeture"] = pd.to_datetime(df["date_fermeture"], format="%Y-%m-%d", errors="coerce")
        df["annee_fermeture"] = df["date_fermeture"].dt.year
    
    # 5. Normalisation des codes postaux
    if "code_postal" in df.columns:
        df["code_postal"] = df["code_postal"].astype(str).str.strip()
        df["code_postal"] = df["code_postal"].replace("nan", "").replace("", None)
        df["code_postal"] = df["code_postal"].fillna("00000")
    
    # 6. Filtrage sur la colonne "etat" pour ne garder que les établissements fermés
    if "etat" in df.columns:
        df = df[df["etat"] == "fermé"]
        print(f"Filtrage des établissements fermés: {len(df)} lignes conservées")
    
    print("\n--- Aperçu des données nettoyées ---")
    print(df.head(5))
    
    return df

def load_data(df: pd.DataFrame, output_folder: str):
    """
    Charge le DataFrame transformé dans un fichier CSV de sortie.
    
    :param df: DataFrame transformé
    :param output_folder: Chemin du dossier de sortie
    """
    # Création du dossier de sortie s'il n'existe pas
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    else:
        print(f"Le dossier de sortie {output_folder} existe déjà. Les données seront écrasées.")
    
    output_path = os.path.join(output_folder, "data_transformed.csv")
    try:
        df.to_csv(output_path, index=False, sep=';', encoding='utf-8')
        print(f"✓ Données sauvegardées dans {output_path} avec le séparateur point-virgule (;)")
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des données : {e}")


def calculate_closed_by_year_and_dept(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le nombre d'établissements fermés par année et par département.
    
    :param df: DataFrame transformé contenant les colonnes 'annee_fermeture' et 'code_departement'
    :return: DataFrame agrégé avec le nombre d'établissements fermés par année et département
    """
    if "annee_fermeture" not in df.columns:
        print("❌ La colonne 'annee_fermeture' est manquante.")
        return pd.DataFrame()

    # Utiliser le code_departement directement
    df['departement'] = df['code_departement']
    
    # Statistiques générales sur les fermetures
    print("\n=== Statistiques sur les fermetures d'établissements ===")
    print(f"Période couverte : de {df['annee_fermeture'].min()} à {df['annee_fermeture'].max()}")
    
    # Top 5 des années avec le plus de fermetures
    yearly_counts = df['annee_fermeture'].value_counts().sort_index()
    print("\nTop 5 des années avec le plus de fermetures :")
    print(yearly_counts.sort_values(ascending=False).head())
    
    # Création du DataFrame groupé avec statistiques séparées par secteur
    df_grouped = (
        df.groupby(["annee_fermeture", "departement"])
          .agg({
              'numero_uai': 'count',
              'libelle_departement': 'first',
              'secteur_public_prive_libe': lambda x: {
                  'nb_public': sum(x.str.lower() == 'public'),
                  'nb_prive': sum(x.str.lower() == 'privé'),
                  'pct_public': round(sum(x.str.lower() == 'public') * 100 / len(x), 2),
                  'pct_prive': round(sum(x.str.lower() == 'privé') * 100 / len(x), 2)
              }
          })
          .reset_index()
    )
    
    # Décomposer le dictionnaire en colonnes
    df_grouped[['nb_public', 'nb_prive', 'pct_public', 'pct_prive']] = pd.DataFrame(
        df_grouped['secteur_public_prive_libe'].tolist(), 
        index=df_grouped.index
    )
    
    # Supprimer la colonne dictionnaire
    df_grouped = df_grouped.drop('secteur_public_prive_libe', axis=1)
    
    # Renommage des colonnes
    df_grouped = df_grouped.rename(columns={
        'numero_uai': 'nombre_total_etablissements',
        'libelle_departement': 'nom_departement'
    })
    
    # Tri par année et département
    df_grouped = df_grouped.sort_values(['annee_fermeture', 'departement'])
    
    print("\nAperçu des statistiques par département et année :")
    print(df_grouped.head(10))
    
    return df_grouped


if __name__ == "__main__":
    main()
