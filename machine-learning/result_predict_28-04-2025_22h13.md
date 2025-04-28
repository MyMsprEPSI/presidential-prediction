# 🧠 Prédiction des résultats politiques

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)

**Accuracy** : `0.6702`

**CV (5 folds)** : `0.6789` ± `0.0732`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.65      0.95      0.77        37
           6       0.70      0.70      0.70        40

    accuracy                           0.67        94
   macro avg       0.45      0.55      0.49        94
weighted avg       0.55      0.67      0.60        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 61.70 %)

**Accuracy** : `0.6596`

**CV (5 folds)** : `0.6522` ± `0.0393`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.60      0.95      0.74        37
           6       0.75      0.68      0.71        40

    accuracy                           0.66        94
   macro avg       0.45      0.54      0.48        94
weighted avg       0.56      0.66      0.59        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 67.02 %)

**Accuracy** : `0.6596`

**CV (5 folds)** : `0.6471` ± `0.0615`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.59      1.00      0.74        37
           6       0.81      0.62      0.70        40

    accuracy                           0.66        94
   macro avg       0.46      0.54      0.48        94
weighted avg       0.57      0.66      0.59        94
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

**Accuracy** : `0.6064`

**CV (5 folds)** : `0.5822` ± `0.0786`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.30      0.41      0.35        17
           4       0.67      0.81      0.73        37
           6       0.77      0.50      0.61        40

    accuracy                           0.61        94
   macro avg       0.58      0.57      0.56        94
weighted avg       0.64      0.61      0.61        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 63.83 %)

**Accuracy** : `0.4362`

**CV (5 folds)** : `0.4919` ± `0.1030`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.42      0.68      0.52        37
           6       0.47      0.40      0.43        40

    accuracy                           0.44        94
   macro avg       0.30      0.36      0.32        94
weighted avg       0.36      0.44      0.39        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Centre` (ID 4, 50.00 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6630` ± `0.0282`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.66      0.84      0.74        37
           6       0.64      0.75      0.69        40

    accuracy                           0.65        94
   macro avg       0.43      0.53      0.48        94
weighted avg       0.53      0.65      0.58        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 53.19 %)

**Accuracy** : `0.6170`

**CV (5 folds)** : `0.6468` ± `0.0780`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.27      0.18      0.21        17
           4       0.64      0.86      0.74        37
           6       0.70      0.57      0.63        40

    accuracy                           0.62        94
   macro avg       0.54      0.54      0.53        94
weighted avg       0.60      0.62      0.60        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Logistic Regression** — *Régression logistique*

- **Accuracy** : `0.6702`

- **CV (5 folds)** : `0.6789` ± `0.0732`

- **Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes