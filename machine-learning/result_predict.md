# 🧠 Prédiction des résultats politiques

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)

**Accuracy** : `0.6915`

**CV (5 folds)** : `0.6415` ± `0.0337`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.64      0.97      0.77        37
           6       0.76      0.72      0.74        40

    accuracy                           0.69        94
   macro avg       0.47      0.57      0.51        94
weighted avg       0.58      0.69      0.62        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 63.83 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6469` ± `0.0213`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.60      0.97      0.74        37
           6       0.74      0.62      0.68        40

    accuracy                           0.65        94
   macro avg       0.45      0.53      0.47        94
weighted avg       0.55      0.65      0.58        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 67.02 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6256` ± `0.0455`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.57      0.97      0.72        37
           6       0.81      0.62      0.70        40

    accuracy                           0.65        94
   macro avg       0.46      0.53      0.47        94
weighted avg       0.57      0.65      0.58        94
```

---

## 🔹 Gradient Boosting — *Gradient Boosting*

**Parti prédit gagnant** : `Centre` (ID 4, 100.00 %)

**Accuracy** : `0.3936`

**CV (5 folds)** : `0.4279` ± `0.0056`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.39      1.00      0.56        37
           6       0.00      0.00      0.00        40

    accuracy                           0.39        94
   macro avg       0.13      0.33      0.19        94
weighted avg       0.15      0.39      0.22        94
```

---

## 🔹 KNN — *K plus proches voisins*

**Parti prédit gagnant** : `Centre` (ID 4, 46.81 %)

**Accuracy** : `0.5638`

**CV (5 folds)** : `0.4869` ± `0.0852`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.33      0.59      0.43        17
           4       0.59      0.70      0.64        37
           6       0.85      0.42      0.57        40

    accuracy                           0.56        94
   macro avg       0.59      0.57      0.54        94
weighted avg       0.65      0.56      0.57        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 65.96 %)

**Accuracy** : `0.4468`

**CV (5 folds)** : `0.4654` ± `0.0530`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.40      0.68      0.51        37
           6       0.53      0.42      0.47        40

    accuracy                           0.45        94
   macro avg       0.31      0.37      0.33        94
weighted avg       0.38      0.45      0.40        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Droite` (ID 6, 52.13 %)

**Accuracy** : `0.5851`

**CV (5 folds)** : `0.6199` ± `0.0460`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.62      0.76      0.68        37
           6       0.55      0.68      0.61        40

    accuracy                           0.59        94
   macro avg       0.39      0.48      0.43        94
weighted avg       0.48      0.59      0.53        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 58.51 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6037` ± `0.0503`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.56      0.29      0.38        17
           4       0.60      0.89      0.72        37
           6       0.77      0.57      0.66        40

    accuracy                           0.65        94
   macro avg       0.64      0.59      0.59        94
weighted avg       0.66      0.65      0.63        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Logistic Regression** — *Régression logistique*

- **Accuracy** : `0.6915`

- **CV (5 folds)** : `0.6415` ± `0.0337`

- **Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes