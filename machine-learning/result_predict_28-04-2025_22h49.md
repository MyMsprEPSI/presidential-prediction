# 🧠 Prédiction des résultats politiques

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 56.38 %)

**Accuracy** : `0.7021`

**CV (5 folds)** : `0.6630` ± `0.0367`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.66      0.95      0.78        37
           6       0.76      0.78      0.77        40

    accuracy                           0.70        94
   macro avg       0.47      0.57      0.51        94
weighted avg       0.58      0.70      0.63        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 64.89 %)

**Accuracy** : `0.6702`

**CV (5 folds)** : `0.6578` ± `0.0393`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.59      0.97      0.73        37
           6       0.82      0.68      0.74        40

    accuracy                           0.67        94
   macro avg       0.47      0.55      0.49        94
weighted avg       0.58      0.67      0.60        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 61.70 %)

**Accuracy** : `0.6809`

**CV (5 folds)** : `0.6578` ± `0.0310`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.60      0.95      0.74        37
           6       0.81      0.72      0.76        40

    accuracy                           0.68        94
   macro avg       0.47      0.56      0.50        94
weighted avg       0.58      0.68      0.61        94
```

---

## 🔹 Gradient Boosting — *Gradient Boosting*

**Parti prédit gagnant** : `Centre` (ID 4, 100.00 %)

**Accuracy** : `0.3936`

**CV (5 folds)** : `0.4755` ± `0.0688`


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

**Parti prédit gagnant** : `Centre` (ID 4, 47.87 %)

**Accuracy** : `0.5851`

**CV (5 folds)** : `0.5350` ± `0.0365`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.19      0.24      0.21        17
           4       0.69      0.84      0.76        37
           6       0.71      0.50      0.59        40

    accuracy                           0.59        94
   macro avg       0.53      0.52      0.52        94
weighted avg       0.61      0.59      0.59        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 70.21 %)

**Accuracy** : `0.3936`

**CV (5 folds)** : `0.4808` ± `0.0385`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.38      0.68      0.49        37
           6       0.43      0.30      0.35        40

    accuracy                           0.39        94
   macro avg       0.27      0.33      0.28        94
weighted avg       0.33      0.39      0.34        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Centre` (ID 4, 60.64 %)

**Accuracy** : `0.6277`

**CV (5 folds)** : `0.6418` ± `0.0350`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.58      0.89      0.70        37
           6       0.70      0.65      0.68        40

    accuracy                           0.63        94
   macro avg       0.43      0.51      0.46        94
weighted avg       0.53      0.63      0.56        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 58.51 %)

**Accuracy** : `0.6915`

**CV (5 folds)** : `0.6363` ± `0.0283`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.38      0.18      0.24        17
           4       0.64      0.95      0.76        37
           6       0.87      0.68      0.76        40

    accuracy                           0.69        94
   macro avg       0.63      0.60      0.59        94
weighted avg       0.69      0.69      0.67        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Logistic Regression** — *Régression logistique*

- **Accuracy** : `0.7021`

- **CV (5 folds)** : `0.6630` ± `0.0367`

- **Parti prédit gagnant** : `Centre` (ID 4, 56.38 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes