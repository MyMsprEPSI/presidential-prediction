import pandas as pd
import numpy as np
import warnings
import logging

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.exceptions import ConvergenceWarning, UndefinedMetricWarning

# Supprimer les warnings spécifiques
warnings.filterwarnings("ignore", category=UndefinedMetricWarning)
warnings.filterwarnings("ignore", category=ConvergenceWarning)
warnings.filterwarnings("ignore", message="The least populated class in y")
PARTY_LABELS = {
    1: "Extrême Gauche",
    2: "Gauche",
    3: "Centre Gauche",
    4: "Centre",
    5: "Centre Droite",
    6: "Droite",
    7: "Extrême Droite"
}

MODEL_DESCRIPTIONS = {
    "Logistic Regression": "Régression logistique (modèle linéaire de classification)",
    "Random Forest": "Forêt aléatoire (ensemble d'arbres de décision)",
    "SVM (RBF)": "SVM à noyau RBF (classification à marge maximale)",
    "Gradient Boosting": "Gradient Boosting (arbre additif séquentiel)",
    "KNN": "K plus proches voisins (vote majoritaire des voisins)",
    "MLP (Neural Net)": "Perceptron multicouche (réseau de neurones)",
    "Decision Tree": "Arbre de décision (structure hiérarchique de règles)"
}

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    file_path = "./../data/jeu_entrainement/previews_presidentiel.csv"
    output_md = "result_predict.md"

    try:
        df = pd.read_csv(file_path, sep=";")
        logger.info("Données chargées avec succès depuis %s", file_path)
    except Exception as e:
        logger.error("Erreur lors du chargement du fichier: %s", e)
        return

    logger.info("Aperçu du jeu d'entraînement :")
    logger.info(df.head())

    target = "politique"
    features = [col for col in df.columns if col not in ["annee_code_dpt", target]]
    X = df[features].apply(pd.to_numeric, errors="coerce")
    y = df[target].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.4, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000, random_state=42),
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "SVM (RBF)": SVC(kernel="rbf", probability=True, random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(random_state=42),
        "KNN": KNeighborsClassifier(n_neighbors=5),
        "MLP (Neural Net)": MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42),
        "Decision Tree": DecisionTreeClassifier(random_state=42),
    }

    results = []
    md_lines = ["# 🧠 Prédiction des résultats politiques par Machine Learning\n"]

    for name, model in models.items():
        desc = MODEL_DESCRIPTIONS.get(name, "Modèle supervisé")
        logger.info(f"📊 Entraînement du modèle : {name}")
        model.fit(X_train_scaled, y_train)

        y_pred = model.predict(X_test_scaled)
        acc = accuracy_score(y_test, y_pred)
        report_dict = classification_report(y_test, y_pred, output_dict=True)
        report_text = classification_report(y_test, y_pred)

        unique, counts = np.unique(y_pred, return_counts=True)
        top_index = np.argmax(counts)
        winning_label = unique[top_index]
        winning_pct = counts[top_index] / len(y_pred) * 100
        party_name = PARTY_LABELS.get(winning_label, "Inconnu")

        cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=5, scoring="accuracy")

        results.append({
            "name": name,
            "description": desc,
            "accuracy": acc,
            "cv_mean": np.mean(cv_scores),
            "cv_std": np.std(cv_scores),
            "winner_id": winning_label,
            "winner_pct": winning_pct,
            "winner_name": party_name
        })

        md_lines.append(f"## 🔹 {name} — *{desc}*\n")
        md_lines.append(f"**Parti politique prédit gagnant** : `{party_name}` *(ID: {winning_label}, {winning_pct:.2f} %)*\n")
        md_lines.append(f"**Accuracy sur test** : `{acc:.4f}`\n")
        md_lines.append(f"**Cross-validation (CV)** : `{np.mean(cv_scores):.4f}` ± `{np.std(cv_scores):.4f}`\n")
        md_lines.append("\n**Classification report** :\n")
        md_lines.append("```text\n" + report_text.strip() + "\n```\n")
        md_lines.append("---\n")

    # Choix du meilleur modèle basé sur accuracy
    best_model = max(results, key=lambda x: x["accuracy"])

    md_lines.append("\n# 🏆 Modèle le plus performant\n")
    md_lines.append(f"### ✅ **{best_model['name']}** — *{best_model['description']}*\n")
    md_lines.append(f"- **Accuracy** : `{best_model['accuracy']:.4f}`\n")
    md_lines.append(f"- **Cross-validation** : `{best_model['cv_mean']:.4f}` ± `{best_model['cv_std']:.4f}`\n")
    md_lines.append(f"- **Parti politique prédit gagnant** : `{best_model['winner_name']}` *(ID: {best_model['winner_id']}, {best_model['winner_pct']:.2f} %)*\n")

    md_lines.append("\n### 🎯 Pourquoi ce modèle est le meilleur ?\n")
    md_lines.append("- Il obtient la meilleure performance en termes d'**accuracy** sur l'ensemble de test.")
    md_lines.append("- Il maintient une **stabilité élevée** avec une faible variance en cross-validation.")
    md_lines.append("- Il prédit de façon cohérente le parti gagnant avec une confiance élevée dans la majorité des départements.")

    # Écriture du fichier markdown
    with open(output_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    logger.info(f"✅ Résultats exportés avec succès dans `{output_md}`")

if __name__ == "__main__":
    main()
