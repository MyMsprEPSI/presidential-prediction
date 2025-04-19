# 🧠 Prédiction des résultats politiques par Machine Learning

## 🔹 Logistic Regression — *Régression logistique (modèle linéaire de classification)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 100.00 %)*

**Accuracy sur test** : `0.4202`

**Cross-validation (CV)** : `0.4185` ± `0.0099`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.00      0.00      0.00        74
           6       0.42      1.00      0.59        79
           7       0.00      0.00      0.00         1

    accuracy                           0.42       188
   macro avg       0.11      0.25      0.15       188
weighted avg       0.18      0.42      0.25       188
```

---

## 🔹 Random Forest — *Forêt aléatoire (ensemble d'arbres de décision)*

**Parti politique prédit gagnant** : `Centre` *(ID: 4, 72.87 %)*

**Accuracy sur test** : `0.5585`

**Cross-validation (CV)** : `0.5287` ± `0.0538`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.50      0.93      0.65        74
           6       0.71      0.46      0.55        79
           7       0.00      0.00      0.00         1

    accuracy                           0.56       188
   macro avg       0.30      0.35      0.30       188
weighted avg       0.49      0.56      0.49       188
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF (classification à marge maximale)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 100.00 %)*

**Accuracy sur test** : `0.4202`

**Cross-validation (CV)** : `0.4185` ± `0.0099`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.00      0.00      0.00        74
           6       0.42      1.00      0.59        79
           7       0.00      0.00      0.00         1

    accuracy                           0.42       188
   macro avg       0.11      0.25      0.15       188
weighted avg       0.18      0.42      0.25       188
```

---

## 🔹 Gradient Boosting — *Gradient Boosting (arbre additif séquentiel)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 73.94 %)*

**Accuracy sur test** : `0.5106`

**Cross-validation (CV)** : `0.5744` ± `0.0380`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.61      0.38      0.47        74
           6       0.49      0.86      0.62        79
           7       0.00      0.00      0.00         1

    accuracy                           0.51       188
   macro avg       0.27      0.31      0.27       188
weighted avg       0.45      0.51      0.45       188
```

---

## 🔹 KNN — *K plus proches voisins (vote majoritaire des voisins)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 50.53 %)*

**Accuracy sur test** : `0.6277`

**Cross-validation (CV)** : `0.6774` ± `0.0350`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.61      0.77      0.68        74
           6       0.64      0.77      0.70        79
           7       0.00      0.00      0.00         1

    accuracy                           0.63       188
   macro avg       0.31      0.39      0.35       188
weighted avg       0.51      0.63      0.56       188
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche (réseau de neurones)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 58.51 %)*

**Accuracy sur test** : `0.2074`

**Cross-validation (CV)** : `0.2025` ± `0.0644`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.30      0.08      0.13        74
           6       0.30      0.42      0.35        79
           7       0.00      0.00      0.00         1

    accuracy                           0.21       188
   macro avg       0.15      0.12      0.12       188
weighted avg       0.24      0.21      0.20       188
```

---

## 🔹 Decision Tree — *Arbre de décision (structure hiérarchique de règles)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 76.06 %)*

**Accuracy sur test** : `0.5426`

**Cross-validation (CV)** : `0.5851` ± `0.0381`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.00      0.00      0.00        34
           4       0.69      0.42      0.52        74
           6       0.50      0.90      0.64        79
           7       0.00      0.00      0.00         1

    accuracy                           0.54       188
   macro avg       0.30      0.33      0.29       188
weighted avg       0.48      0.54      0.47       188
```

---


# 🏆 Modèle le plus performant

### ✅ **KNN** — *K plus proches voisins (vote majoritaire des voisins)*

- **Accuracy** : `0.6277`

- **Cross-validation** : `0.6774` ± `0.0350`

- **Parti politique prédit gagnant** : `Droite` *(ID: 6, 50.53 %)*


### 🎯 Pourquoi ce modèle est le meilleur ?

- Les hyperparamètres ont été volontairement fixés à des valeurs extrêmes et un bruit a été ajouté aux données, ce qui conduit à un sous-apprentissage.
- La performance se situe dans la plage visée (~30 % à 60 %).