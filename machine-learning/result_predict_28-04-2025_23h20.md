# 🧠 Prédiction des résultats politiques

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)

**Accuracy** : `0.6809`

**CV (5 folds)** : `0.6576` ± `0.0403`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.67      0.97      0.79        37
           6       0.70      0.70      0.70        40

    accuracy                           0.68        94
   macro avg       0.46      0.56      0.50        94
weighted avg       0.56      0.68      0.61        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 72.34 %)

**Accuracy** : `0.6170`

**CV (5 folds)** : `0.6468` ± `0.0412`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.53      0.97      0.69        37
           6       0.85      0.55      0.67        40

    accuracy                           0.62        94
   macro avg       0.46      0.51      0.45        94
weighted avg       0.57      0.62      0.55        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Centre` (ID 4, 62.77 %)

**Accuracy** : `0.6809`

**CV (5 folds)** : `0.6578` ± `0.0516`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.61      0.97      0.75        37
           6       0.80      0.70      0.75        40

    accuracy                           0.68        94
   macro avg       0.47      0.56      0.50        94
weighted avg       0.58      0.68      0.61        94
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

**Parti prédit gagnant** : `Centre` (ID 4, 48.94 %)

**Accuracy** : `0.5319`

**CV (5 folds)** : `0.5021` ± `0.0569`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.18      0.29      0.22        17
           4       0.57      0.70      0.63        37
           6       0.95      0.47      0.63        40

    accuracy                           0.53        94
   macro avg       0.56      0.49      0.49        94
weighted avg       0.66      0.53      0.56        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Centre` (ID 4, 77.66 %)

**Accuracy** : `0.4468`

**CV (5 folds)** : `0.4499` ± `0.0870`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.42      0.84      0.56        37
           6       0.52      0.28      0.36        40

    accuracy                           0.45        94
   macro avg       0.32      0.37      0.31        94
weighted avg       0.39      0.45      0.38        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Centre` (ID 4, 73.40 %)

**Accuracy** : `0.6064`

**CV (5 folds)** : `0.5828` ± `0.0369`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.54      1.00      0.70        37
           6       0.80      0.50      0.62        40

    accuracy                           0.61        94
   macro avg       0.45      0.50      0.44        94
weighted avg       0.55      0.61      0.54        94
```

---

## 🔹 Voting Ensemble — *Ensemble par vote (KNN, RF, DT)*

**Parti prédit gagnant** : `Centre` (ID 4, 63.83 %)

**Accuracy** : `0.6489`

**CV (5 folds)** : `0.6148` ± `0.0798`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.62      1.00      0.76        37
           6       0.80      0.60      0.69        40

    accuracy                           0.65        94
   macro avg       0.47      0.53      0.48        94
weighted avg       0.58      0.65      0.59        94
```

---


# 🏆 Modèle le plus performant

### ✅ **Logistic Regression** — *Régression logistique*

- **Accuracy** : `0.6809`

- **CV (5 folds)** : `0.6576` ± `0.0403`

- **Parti prédit gagnant** : `Centre` (ID 4, 57.45 %)


### 🎯 Pourquoi ce modèle performant?

- Hyperparamètres optimisés pour ce petit jeu de données
- Prétraitement adapté à chaque type de modèle
- Techniques spéciales pour gérer le déséquilibre des classes

## 🧪 Prédictions sur années sans présidentielle

### Année 2000

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 98.9%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 93 dép. (98.9%)

- Centre (ID 4): 1 dép. (1.1%)



### Année 2001

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 97.9%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 92 dép. (97.9%)

- Centre (ID 4): 2 dép. (2.1%)



### Année 2003

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 96.8%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 91 dép. (96.8%)

- Centre (ID 4): 3 dép. (3.2%)



### Année 2004

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 93.6%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 88 dép. (93.6%)

- Centre (ID 4): 6 dép. (6.4%)



### Année 2005

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 90.4%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 85 dép. (90.4%)

- Centre (ID 4): 9 dép. (9.6%)



### Année 2006

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 75.5%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 71 dép. (75.5%)

- Centre (ID 4): 23 dép. (24.5%)



### Année 2008

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 63.8%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 60 dép. (63.8%)

- Centre (ID 4): 34 dép. (36.2%)



### Année 2009

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 62.8%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 59 dép. (62.8%)

- Centre (ID 4): 35 dép. (37.2%)



### Année 2010

- **Parti majoritaire** : `Droite` (ID 6)

- **Pourcentage** : 56.4%

- **Nombre de départements** : 94


#### Répartition par parti

- Droite (ID 6): 53 dép. (56.4%)

- Centre (ID 4): 41 dép. (43.6%)



### Année 2011

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 64.9%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 61 dép. (64.9%)

- Droite (ID 6): 33 dép. (35.1%)



### Année 2013

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 72.3%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 68 dép. (72.3%)

- Droite (ID 6): 26 dép. (27.7%)



### Année 2014

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 81.9%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 77 dép. (81.9%)

- Droite (ID 6): 17 dép. (18.1%)



### Année 2015

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 79.8%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 75 dép. (79.8%)

- Droite (ID 6): 19 dép. (20.2%)



### Année 2016

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 83.0%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 78 dép. (83.0%)

- Droite (ID 6): 16 dép. (17.0%)



### Année 2018

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 87.2%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 82 dép. (87.2%)

- Droite (ID 6): 12 dép. (12.8%)



### Année 2019

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 92.6%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 87 dép. (92.6%)

- Droite (ID 6): 7 dép. (7.4%)



### Année 2020

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 79.8%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 75 dép. (79.8%)

- Droite (ID 6): 19 dép. (20.2%)



### Année 2021

- **Parti majoritaire** : `Centre` (ID 4)

- **Pourcentage** : 84.0%

- **Nombre de départements** : 94


#### Répartition par parti

- Centre (ID 4): 79 dép. (84.0%)

- Droite (ID 6): 15 dép. (16.0%)


