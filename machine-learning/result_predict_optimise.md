# 🧠 Prédiction des résultats politiques avec modèles optimisés

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)

**Accuracy** : `0.7340`

**CV (5 folds)** : `0.6738` ± `0.0353`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.69      1.00      0.81        37
           6       0.80      0.80      0.80        40

    accuracy                           0.73        94
   macro avg       0.50      0.60      0.54        94
weighted avg       0.61      0.73      0.66        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6307` ± `0.0591`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.61      0.89      0.73        37
           6       0.70      0.70      0.70        40

    accuracy                           0.65        94
   macro avg       0.44      0.53      0.48        94
weighted avg       0.54      0.65      0.58        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 64.89 %)

**Accuracy** : `0.6915`

**CV (5 folds)** : `0.6363` ± `0.0368`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.61      1.00      0.76        37
           6       0.85      0.70      0.77        40

    accuracy                           0.69        94
   macro avg       0.49      0.57      0.51        94
weighted avg       0.60      0.69      0.62        94
```

---

## 🔹 Gradient Boosting — *Gradient Boosting*

**Parti prédit gagnant** : `Centre` (ID 4, 90.43 %)

**Accuracy** : `0.4894`

**CV (5 folds)** : `0.4865` ± `0.0711`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.44      1.00      0.61        37
           6       1.00      0.23      0.37        40

    accuracy                           0.49        94
   macro avg       0.48      0.41      0.32        94
weighted avg       0.60      0.49      0.40        94
```

---

## 🔹 KNN — *K plus proches voisins*

**Parti prédit gagnant** : `Centre` (ID 4, 46.81 %)

**Accuracy** : `0.5851`

**CV (5 folds)** : `0.5508` ± `0.0574`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.32      0.59      0.42        17
           4       0.64      0.76      0.69        37
           6       0.89      0.42      0.58        40

    accuracy                           0.59        94
   macro avg       0.62      0.59      0.56        94
weighted avg       0.69      0.59      0.59        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 70.21 %)

**Accuracy** : `0.4468`

**CV (5 folds)** : `0.5030` ± `0.1060`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.42      0.76      0.54        37
           6       0.50      0.35      0.41        40

    accuracy                           0.45        94
   macro avg       0.31      0.37      0.32        94
weighted avg       0.38      0.45      0.39        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Droite` (ID 6, 51.06 %)

**Accuracy** : `0.6170`

**CV (5 folds)** : `0.6203` ± `0.0622`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.65      0.81      0.72        37
           6       0.58      0.70      0.64        40

    accuracy                           0.62        94
   macro avg       0.41      0.50      0.45        94
weighted avg       0.50      0.62      0.56        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 55.32 %)

**Accuracy** : `0.6702`

**CV (5 folds)** : `0.6309` ± `0.0758`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.38      0.29      0.33        17
           4       0.65      0.92      0.76        37
           6       0.83      0.60      0.70        40

    accuracy                           0.67        94
   macro avg       0.62      0.60      0.60        94
weighted avg       0.68      0.67      0.66        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Logistic Regression** — *Régression logistique*

- **Accuracy** : `0.7340`

- **CV (5 folds)** : `0.6738` ± `0.0353`

- **Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres soigneusement optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes