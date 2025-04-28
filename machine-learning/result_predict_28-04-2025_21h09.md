# 🧠 Prédiction des résultats politiques

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)

**Accuracy** : `0.6702`

**CV (5 folds)** : `0.6794` ± `0.0565`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.62      0.95      0.75        37
           6       0.74      0.70      0.72        40

    accuracy                           0.67        94
   macro avg       0.45      0.55      0.49        94
weighted avg       0.56      0.67      0.60        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 73.40 %)

**Accuracy** : `0.6064`

**CV (5 folds)** : `0.6313` ± `0.0647`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.52      0.97      0.68        37
           6       0.84      0.53      0.65        40

    accuracy                           0.61        94
   macro avg       0.45      0.50      0.44        94
weighted avg       0.56      0.61      0.54        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 68.09 %)

**Accuracy** : `0.6702`

**CV (5 folds)** : `0.6472` ± `0.0655`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.58      1.00      0.73        37
           6       0.87      0.65      0.74        40

    accuracy                           0.67        94
   macro avg       0.48      0.55      0.49        94
weighted avg       0.60      0.67      0.60        94
```

---

## 🔹 Gradient Boosting — *Gradient Boosting*

**Parti prédit gagnant** : `Centre` (ID 4, 77.66 %)

**Accuracy** : `0.5851`

**CV (5 folds)** : `0.4603` ± `0.0673`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.51      1.00      0.67        37
           6       0.86      0.45      0.59        40

    accuracy                           0.59        94
   macro avg       0.45      0.48      0.42        94
weighted avg       0.56      0.59      0.52        94
```

---

## 🔹 KNN — *K plus proches voisins*

**Parti prédit gagnant** : `Centre` (ID 4, 46.81 %)

**Accuracy** : `0.5638`

**CV (5 folds)** : `0.5131` ± `0.0728`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.26      0.53      0.35        17
           4       0.68      0.81      0.74        37
           6       0.88      0.35      0.50        40

    accuracy                           0.56        94
   macro avg       0.61      0.56      0.53        94
weighted avg       0.69      0.56      0.57        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 60.64 %)

**Accuracy** : `0.5532`

**CV (5 folds)** : `0.4498` ± `0.0565`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.51      0.78      0.62        37
           6       0.62      0.57      0.60        40

    accuracy                           0.55        94
   macro avg       0.38      0.45      0.40        94
weighted avg       0.46      0.55      0.50        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)

**Accuracy** : `0.6596`

**CV (5 folds)** : `0.6202` ± `0.0433`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.61      0.92      0.73        37
           6       0.74      0.70      0.72        40

    accuracy                           0.66        94
   macro avg       0.45      0.54      0.48        94
weighted avg       0.55      0.66      0.59        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 61.70 %)

**Accuracy** : `0.7021`

**CV (5 folds)** : `0.6312` ± `0.0647`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.60      0.18      0.27        17
           4       0.62      0.97      0.76        37
           6       0.87      0.68      0.76        40

    accuracy                           0.70        94
   macro avg       0.70      0.61      0.60        94
weighted avg       0.72      0.70      0.67        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Voting Ensemble** — *Ensemble par vote (KNN, RF, DT)*

- **Accuracy** : `0.7021`

- **CV (5 folds)** : `0.6312` ± `0.0647`

- **Parti prédit gagnant** : `Centre` (ID 4, 61.70 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes