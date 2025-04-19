# 🧠 Prédiction des résultats politiques par Machine Learning

## 🔹 Logistic Regression — *Régression logistique*

**Parti prédit gagnant** : `Droite` (ID 6, 100.00 %)

**Accuracy** : `0.4255`

**CV (2 folds)** : `0.4176` ± `0.0027`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.00      0.00      0.00        37
           6       0.43      1.00      0.60        40

    accuracy                           0.43        94
   macro avg       0.14      0.33      0.20        94
weighted avg       0.18      0.43      0.25        94
```

---

## 🔹 Random Forest — *Forêt aléatoire*

**Parti prédit gagnant** : `Centre` (ID 4, 80.85 %)

**Accuracy** : `0.5106`

**CV (2 folds)** : `0.5160` ± `0.0372`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.47      0.97      0.64        37
           6       0.67      0.30      0.41        40

    accuracy                           0.51        94
   macro avg       0.38      0.42      0.35        94
weighted avg       0.47      0.51      0.43        94
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF*

**Parti prédit gagnant** : `Droite` (ID 6, 100.00 %)

**Accuracy** : `0.4255`

**CV (2 folds)** : `0.4176` ± `0.0027`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.00      0.00      0.00        37
           6       0.43      1.00      0.60        40

    accuracy                           0.43        94
   macro avg       0.14      0.33      0.20        94
weighted avg       0.18      0.43      0.25        94
```

---

## 🔹 Gradient Boosting — *Gradient Boosting*

**Parti prédit gagnant** : `Droite` (ID 6, 77.66 %)

**Accuracy** : `0.5000`

**CV (2 folds)** : `0.5186` ± `0.0186`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.62      0.35      0.45        37
           6       0.47      0.85      0.60        40

    accuracy                           0.50        94
   macro avg       0.36      0.40      0.35        94
weighted avg       0.44      0.50      0.43        94
```

---

## 🔹 KNN — *K plus proches voisins*

**Parti prédit gagnant** : `Centre` (ID 4, 53.19 %)

**Accuracy** : `0.6915`

**CV (2 folds)** : `0.6330` ± `0.0106`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.68      0.92      0.78        37
           6       0.70      0.78      0.74        40

    accuracy                           0.69        94
   macro avg       0.46      0.56      0.51        94
weighted avg       0.57      0.69      0.62        94
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche*

**Parti prédit gagnant** : `Droite` (ID 6, 56.38 %)

**Accuracy** : `0.2766`

**CV (2 folds)** : `0.1489` ± `0.0053`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.54      0.19      0.28        37
           6       0.36      0.47      0.41        40
           7       0.00      0.00      0.00         0

    accuracy                           0.28        94
   macro avg       0.22      0.17      0.17        94
weighted avg       0.36      0.28      0.28        94
```

---

## 🔹 Decision Tree — *Arbre de décision*

**Parti prédit gagnant** : `Centre` (ID 4, 75.53 %)

**Accuracy** : `0.5426`

**CV (2 folds)** : `0.4840` ± `0.0053`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        17
           4       0.51      0.97      0.67        37
           6       0.65      0.38      0.48        40

    accuracy                           0.54        94
   macro avg       0.39      0.45      0.38        94
weighted avg       0.48      0.54      0.47        94
```

---


# 🏆 Modèle le plus performant

### ✅ **KNN** — *K plus proches voisins*

- **Accuracy** : `0.6915`

- **CV (2 folds)** : `0.6330` ± `0.0106`

- **Parti prédit gagnant** : `Centre` (ID 4, 53.19 %)


### 🎯 Pourquoi ?

- Hyperparamètres volontairement faibles + bruit = sous-apprentissage contrôlé.
- Vérification de la robustesse du pipeline malgré conditions dégradées.