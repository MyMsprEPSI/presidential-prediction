# 🧠 Prédiction des résultats politiques

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)

**Accuracy** : `0.7021`

**CV (5 folds)** : `0.6363` ± `0.0505`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.66      1.00      0.80        37
           6       0.76      0.72      0.74        40

    accuracy                           0.70        94
   macro avg       0.47      0.58      0.51        94
weighted avg       0.58      0.70      0.63        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)

**Accuracy** : `0.6064`

**CV (5 folds)** : `0.6364` ± `0.0430`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.59      0.89      0.71        37
           6       0.63      0.60      0.62        40

    accuracy                           0.61        94
   macro avg       0.41      0.50      0.44        94
weighted avg       0.50      0.61      0.54        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 67.02 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6523` ± `0.0455`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.59      1.00      0.74        37
           6       0.77      0.60      0.68        40

    accuracy                           0.65        94
   macro avg       0.45      0.53      0.47        94
weighted avg       0.56      0.65      0.58        94
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

**Parti prédit gagnant** : `Centre` (ID 4, 47.87 %)

**Accuracy** : `0.6383`

**CV (5 folds)** : `0.5883` ± `0.0251`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.40      0.59      0.48        17
           4       0.71      0.86      0.78        37
           6       0.75      0.45      0.56        40

    accuracy                           0.64        94
   macro avg       0.62      0.63      0.61        94
weighted avg       0.67      0.64      0.63        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 60.64 %)

**Accuracy** : `0.4468`

**CV (5 folds)** : `0.5031` ± `0.0502`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.42      0.65      0.51        37
           6       0.49      0.45      0.47        40

    accuracy                           0.45        94
   macro avg       0.30      0.37      0.33        94
weighted avg       0.37      0.45      0.40        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Centre` (ID 4, 51.06 %)

**Accuracy** : `0.6064`

**CV (5 folds)** : `0.6366` ± `0.0290`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.65      0.84      0.73        37
           6       0.57      0.65      0.60        40

    accuracy                           0.61        94
   macro avg       0.40      0.50      0.44        94
weighted avg       0.49      0.61      0.54        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 53.19 %)

**Accuracy** : `0.6596`

**CV (5 folds)** : `0.6687` ± `0.0411`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.46      0.35      0.40        17
           4       0.68      0.92      0.78        37
           6       0.71      0.55      0.62        40

    accuracy                           0.66        94
   macro avg       0.62      0.61      0.60        94
weighted avg       0.65      0.66      0.64        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Logistic Regression** — *Régression logistique*

- **Accuracy** : `0.7021`

- **CV (5 folds)** : `0.6363` ± `0.0505`

- **Parti prédit gagnant** : `Centre` (ID 4, 59.57 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes