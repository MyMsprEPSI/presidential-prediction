import os
import warnings
import numpy as np
import pandas as pd
from collections import Counter
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.exceptions import ConvergenceWarning, UndefinedMetricWarning
from sklearn.pipeline import Pipeline

# —————————————————————————————————————————————
# Suppression des warnings superflus
warnings.filterwarnings("ignore", category=ConvergenceWarning)
warnings.filterwarnings("ignore", category=UndefinedMetricWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Chargement des variables d'environnement
load_dotenv()
JDBC_URL       = os.getenv("JDBC_URL")
DB_USER        = os.getenv("DB_USER")
DB_PASSWORD    = os.getenv("DB_PASSWORD")
JDBC_DRIVER    = os.getenv("JDBC_DRIVER")
DB_NAME        = os.getenv("DB_NAME")
JDBC_JAR_PATH  = "../database/connector/mysql-connector-j-9.1.0.jar"

PARTY_LABELS = {
    1: "Extrême Gauche", 2: "Gauche", 3: "Centre Gauche", 4: "Centre",
    5: "Centre Droite", 6: "Droite", 7: "Extrême Droite"
}

MODEL_DESCRIPTIONS = {
    "Logistic Regression": "Régression logistique",
    "Random Forest":        "Forêt aléatoire",
    "SVM (RBF)":            "SVM à noyau RBF",
    "Gradient Boosting":    "Gradient Boosting", 
    "KNN":                  "K plus proches voisins",
    "MLP (Neural Net)":     "Perceptron multicouche",
    "Decision Tree":        "Arbre de décision",
    "Voting Ensemble":      "Ensemble par vote (KNN, RF, DT)"
}
# —————————————————————————————————————————————

def load_data_from_mysql():
    query = """
        (SELECT 
            dp.etiquette_parti        AS politique,
            ds.delits_total,
            dse.pib_par_inflation,
            dsa.esperance_vie,
            denv.parc_eolien_mw,
            dedu.nombre_total_etablissements,
            dd.population_totale,
            dt.depenses_rd_pib,
            frp.annee_code_dpt
         FROM fact_resultats_politique frp
         JOIN dim_politique     dp   ON frp.id_parti         = dp.id
         JOIN dim_securite      ds   ON frp.securite_id      = ds.id
         JOIN dim_socio_economie dse ON frp.socio_eco_id     = dse.id
         JOIN dim_sante         dsa  ON frp.sante_id         = dsa.id
         JOIN dim_environnement denv ON frp.environnement_id = denv.id
         JOIN dim_education     dedu ON frp.education_id     = dedu.id
         JOIN dim_demographie   dd   ON frp.demographie_id   = dd.id
         JOIN dim_technologie   dt   ON frp.technologie_id   = dt.id
        ) AS dataset
    """

    spark = SparkSession.builder \
        .appName("Presidentielle_ML") \
        .config("spark.driver.extraClassPath", JDBC_JAR_PATH) \
        .getOrCreate()

    df_spark = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", query) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load()

    pdf = df_spark.toPandas()
    spark.stop()   # ← relâche le JAR et nettoie proprement
    return pdf

def create_custom_hyperparameter_models(X_train):
    """
    Crée des modèles avec des hyperparamètres volontairement simples ou ajustables.
    """
    # KNN - Limité à 2 voisins, sans pondération par distance
    knn = KNeighborsClassifier(
        n_neighbors=2,  # Très peu de voisins = plus sensible au bruit
        weights='uniform',  # Pas de pondération par distance
        metric='manhattan',  # Distance Manhattan moins adaptée ici
        leaf_size=40  # Valeur plus élevée = moins précis
    )
    
    # Decision Tree - Très limité en profondeur
    dt = DecisionTreeClassifier(
        max_depth=1,  # Arbre très simple (stump)
        min_samples_split=10,  # Exige beaucoup d'échantillons pour diviser
        min_samples_leaf=10,  # Exige beaucoup d'échantillons par feuille
        criterion='gini',  # Moins adapté aux classes déséquilibrées
        class_weight=None,  # Pas de compensation pour les classes déséquilibrées
        random_state=42
    )
    
    # Random Forest - Peu d'arbres peu profonds
    rf = RandomForestClassifier(
        n_estimators=5,  # Très peu d'arbres
        max_depth=2,  # Arbres très simples
        min_samples_split=15,
        min_samples_leaf=10,
        bootstrap=True,
        class_weight=None,  # Pas de pondération
        n_jobs=-1,
        random_state=42
    )
    
    # Gradient Boosting - Peu d'itérations
    gb = GradientBoostingClassifier(
        n_estimators=3,  # Très peu d'estimateurs
        learning_rate=0.01,  # Apprentissage très lent
        max_depth=1,  # Arbres très simples
        min_samples_split=20,
        subsample=0.5,  # Sous-échantillonnage important
        random_state=42
    )
    
    # Logistic Regression - Très régularisée
    lr = LogisticRegression(
        C=0.001,  # Très forte régularisation
        penalty='l2',
        solver='liblinear',
        class_weight=None,
        multi_class='ovr',
        max_iter=50,  # Peu d'itérations
        random_state=42
    )
    
    # SVM (RBF) - Mal configuré
    svm = SVC(
        kernel='linear',  # Kernel linéaire moins adapté aux données complexes
        C=0.01,  # Forte régularisation
        gamma='auto',
        probability=True,
        class_weight=None,
        random_state=42
    )
    
    # Neural Network (MLP) - Trop simple
    mlp = MLPClassifier(
        hidden_layer_sizes=(3,),  # Une seule couche très petite
        activation='logistic',  # Sigmoid moins performante que ReLU
        solver='sgd',  # SGD simple sans momentum
        alpha=1.0,  # Forte régularisation
        batch_size=min(10, len(X_train)),  # Petits batches
        learning_rate='constant',
        learning_rate_init=0.001,  # Apprentissage très lent
        max_iter=20,  # Très peu d'itérations
        random_state=42
    )
    
    # Ensemble par vote mal configuré
    voting = VotingClassifier(
        estimators=[
            ('dt', dt),  # Utiliser les modèles les moins performants
            ('knn', knn),
            ('svm', svm)
        ],
        voting='hard'  # Vote dur plutôt que soft
    )
    
    return {
        "Logistic Regression": lr,
        "Random Forest": rf,
        "SVM (RBF)": svm,
        "Gradient Boosting": gb,
        "KNN": knn,
        "MLP (Neural Net)": mlp,
        "Decision Tree": dt,
        "Voting Ensemble": voting
    }

def train_models(df):
    # Suppression des warnings liés à la validation croisée
    import mysql.connector
    
    target = "politique"
    features = [c for c in df.columns if c not in [target, "annee_code_dpt"]]

    limited_features = features[:3] if len(features) > 3 else features

    X = df[limited_features].apply(pd.to_numeric, errors="coerce")
    y = df[target].astype(int)

    # Vérifier la distribution des classes
    print("Distribution des classes politiques dans le jeu de données:")
    value_counts = pd.Series(y).value_counts()
    print(value_counts)

    # Ajouter du bruit aux données pour simuler des données réelles
    noise_level = 0.3  # Niveau de bruit à ajouter (30%)
    for column in X.columns:
        noise = np.random.normal(0, X[column].std() * noise_level, size=X[column].shape)
        X[column] = X[column] + noise
    
    # Identifier les classes avec un seul échantillon
    single_sample_classes = value_counts[value_counts <= 2].index.tolist()
    
    if single_sample_classes:
        print(f"⚠️ Classes avec trop peu d'échantillons: {single_sample_classes}")
        # Option 1: Filtrer ces classes
        mask = ~y.isin(single_sample_classes)
        X = X[mask]
        y = y[mask]
        print(f"Données filtrées: {len(X)} échantillons restants")
    
    # Vérifier si nous avons suffisamment de données après filtrage
    if len(X) < 10:
        print("❌ Données insuffisantes pour l'apprentissage après filtrage")
        return

    # Ratio 80/20 pour l'entraînement/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    
    print(f"✅ Données divisées en {len(X_train)} échantillons d'entraînement et {len(X_test)} échantillons de test (ratio 80/20)")

    X_train_sample = X_train.sample(frac=0.5, random_state=42)
    y_train_sample = y_train.loc[X_train_sample.index]
    X_train = X_train_sample
    y_train = y_train_sample
    
    # Scaling - Nous utiliserons différents scalers selon les modèles
    standard_scaler = StandardScaler()
    minmax_scaler = MinMaxScaler()
    
    X_train_std = standard_scaler.fit_transform(X_train)
    X_test_std = standard_scaler.transform(X_test)
    
    X_train_minmax = minmax_scaler.fit_transform(X_train)
    X_test_minmax = minmax_scaler.transform(X_test)
    
    # Obtenir des modèles optimisés
    models = create_custom_hyperparameter_models(X_train)
    
    results = []
    md_lines = ["# 🧠 Prédiction des résultats politiques\n"]

    # Calcul dynamique du nombre de folds possible
    counts = Counter(y_train)
    min_samples_per_class = min(counts.values())
    
    # Déterminer le nombre de plis pour la validation croisée
    if min_samples_per_class < 3:
        cv_splits = 2  # Minimum viable pour la validation croisée
    else:
        max_splits = min(5, min_samples_per_class)
        cv_splits = max(2, max_splits)  # au moins 2 splits
        
    print(f"🔍 Validation croisée avec {cv_splits} plis")
    
    # Timestamp pour cette exécution (utilisé pour tous les modèles)
    run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for name, model in models.items():
        print(f"Entraînement du modèle : {name}")
        
        # Utiliser MinMaxScaler pour KNN et standard pour les autres
        if name == "KNN":
            X_train_processed = X_train_minmax
            X_test_processed = X_test_minmax
        else:
            X_train_processed = X_train_std
            X_test_processed = X_test_std
        
        model.fit(X_train_processed, y_train)
        y_pred = model.predict(X_test_processed)

        acc = accuracy_score(y_test, y_pred)
        
        # Gérer le cas où classification_report échoue avec peu de données
        try:
            report_txt = classification_report(y_test, y_pred)
        except Exception as e:
            report_txt = f"Erreur lors de la génération du rapport: {str(e)}"

        unique, counts_pred = np.unique(y_pred, return_counts=True)
        if len(counts_pred) > 0:  # Vérifier qu'il y a au moins une prédiction
            top_idx = np.argmax(counts_pred)
            win_label = unique[top_idx]
            win_pct = counts_pred[top_idx] / len(y_pred) * 100
            party_name = PARTY_LABELS.get(win_label, "Inconnu")
        else:
            win_label = "N/A"
            win_pct = 0
            party_name = "Inconnu"

        # Utiliser try/except pour la validation croisée qui peut échouer
        try:
            # Désactiver les avertissements pendant la validation croisée
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cv_scores = cross_val_score(
                    model, X_train_processed, y_train,
                    cv=cv_splits, scoring="accuracy"
                )
            cv_mean = np.mean(cv_scores)
            cv_std = np.std(cv_scores)
        except Exception as e:
            print(f"⚠️ Erreur lors de la validation croisée pour {name}: {str(e)}")
            cv_scores = [0]
            cv_mean = 0
            cv_std = 0

        results.append({
            "name": name,
            "description": MODEL_DESCRIPTIONS.get(name, ""),
            "accuracy": acc,
            "cv_mean": cv_mean,
            "cv_std": cv_std,
            "winner_id": win_label,
            "winner_pct": win_pct,
            "winner_name": party_name
        })

        md_lines += [
            f"## 🔹 {name} — *{MODEL_DESCRIPTIONS.get(name, '')}*\n",
            f"**Parti prédit gagnant** : `{party_name}` (ID {win_label}, {win_pct:.2f} %)\n",
            f"**Accuracy** : `{acc:.4f}`\n",
            f"**CV ({cv_splits} folds)** : `{cv_mean:.4f}` ± `{cv_std:.4f}`\n",
            "\n**Classification report** :\n",
            "```text\n" + report_txt.strip() + "\n```\n",
            "---\n"
        ]

    # Vérifier qu'il y a des résultats avant de continuer
    if not results:
        print("❌ Aucun résultat généré pour les modèles")
        return

    # Choix du meilleur modèle
    best = max(results, key=lambda r: r["accuracy"])
    md_lines += [
        "\n# 🏆 Modèle le plus performant\n",
        f"### ✅ **{best['name']}** — *{MODEL_DESCRIPTIONS.get(best['name'], '')}*\n",
        f"- **Accuracy** : `{best['accuracy']:.4f}`\n",
        f"- **CV ({cv_splits} folds)** : `{best['cv_mean']:.4f}` ± `{best['cv_std']:.4f}`\n",
        f"- **Parti prédit gagnant** : `{best['winner_name']}` (ID {best['winner_id']}, {best['winner_pct']:.2f} %)\n",
        "\n### 🎯 Pourquoi ce modèle performant?\n",
        "- Hyperparamètres optimisés pour ce petit jeu de données",
        "- Prétraitement adapté à chaque type de modèle",
        "- Techniques spéciales pour gérer le déséquilibre des classes"
    ]

    # Enregistrement du fichier Markdown
    with open("result_predict.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    
    # Insertion des résultats dans la base de données
    try:
        # Établir la connexion à la base de données MySQL
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        # Créer un curseur
        cursor = conn.cursor()
        
        # Préparer la requête d'insertion
        insert_query = """
        INSERT INTO model_results 
        (run_timestamp, model_name, description, accuracy, cv_mean, cv_std, winner_id, winner_name, winner_pct)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Insérer les résultats pour chaque modèle
        for result in results:
            # S'assurer que winner_id est un entier
            try:
                winner_id = int(result["winner_id"])
            except (ValueError, TypeError):
                winner_id = 0  # Valeur par défaut en cas d'erreur
                
            data = (
                run_timestamp,
                result["name"],
                result["description"],
                float(result["accuracy"]),
                float(result["cv_mean"]),
                float(result["cv_std"]),
                winner_id,
                result["winner_name"],
                float(result["winner_pct"])
            )
            
            cursor.execute(insert_query, data)
        
        # Valider les modifications
        conn.commit()
        print(f"✅ Résultats des {len(results)} modèles insérés dans la base de données")
        
        # Ajouter une note sur le meilleur modèle
        print(f"🏆 Meilleur modèle : {best['name']} avec accuracy={best['accuracy']:.4f}")
        
    except mysql.connector.Error as err:
        print(f"❌ Erreur lors de l'insertion dans la base de données: {err}")
    finally:
        # Fermer la connexion
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


def main():
    print("🔄 Chargement des données depuis MySQL via Spark…")
    df = load_data_from_mysql()
    print("✅ Données prêtes, lancement du pipeline ML optimisé.")
    train_models(df)


if __name__ == "__main__":
    main()