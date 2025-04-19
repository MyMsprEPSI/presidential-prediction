# 🧠 Prédiction des résultats politiques par Machine Learning

## 🔹 Logistic Regression — *Régression logistique (modèle linéaire de classification)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 49.47 %)*

**Accuracy sur test** : `0.7234`

**Cross-validation (CV)** : `0.7482` ± `0.0180`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.56      0.15      0.23        34
           4       0.76      0.88      0.82        74
           6       0.71      0.84      0.77        79
           7       0.00      0.00      0.00         1

    accuracy                           0.72       188
   macro avg       0.51      0.47      0.45       188
weighted avg       0.70      0.72      0.69       188
```

---

## 🔹 Random Forest — *Forêt aléatoire (ensemble d'arbres de décision)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 43.09 %)*

**Accuracy sur test** : `0.8564`

**Cross-validation (CV)** : `0.8581` ± `0.0249`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.62      0.59      0.61        34
           4       0.99      1.00      0.99        74
           6       0.83      0.85      0.84        79
           7       0.00      0.00      0.00         1

    accuracy                           0.86       188
   macro avg       0.61      0.61      0.61       188
weighted avg       0.85      0.86      0.85       188
```

---

## 🔹 SVM (RBF) — *SVM à noyau RBF (classification à marge maximale)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 44.68 %)*

**Accuracy sur test** : `0.7713`

**Cross-validation (CV)** : `0.7413` ± `0.0221`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.62      0.47      0.53        34
           4       0.85      0.89      0.87        74
           6       0.75      0.80      0.77        79
           7       0.00      0.00      0.00         1

    accuracy                           0.77       188
   macro avg       0.55      0.54      0.54       188
weighted avg       0.76      0.77      0.76       188
```

---

## 🔹 Gradient Boosting — *Gradient Boosting (arbre additif séquentiel)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 43.62 %)*

**Accuracy sur test** : `0.8670`

**Cross-validation (CV)** : `0.8583` ± `0.0349`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.68      0.62      0.65        34
           4       0.99      0.99      0.99        74
           6       0.84      0.87      0.86        79
           7       0.00      0.00      0.00         1

    accuracy                           0.87       188
   macro avg       0.63      0.62      0.62       188
weighted avg       0.86      0.87      0.87       188
```

---

## 🔹 KNN — *K plus proches voisins (vote majoritaire des voisins)*

**Parti politique prédit gagnant** : `Centre` *(ID: 4, 41.49 %)*

**Accuracy sur test** : `0.7766`

**Cross-validation (CV)** : `0.7518` ± `0.0115`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.51      0.68      0.58        34
           4       0.85      0.89      0.87        74
           6       0.88      0.72      0.79        79
           7       0.00      0.00      0.00         1

    accuracy                           0.78       188
   macro avg       0.56      0.57      0.56       188
weighted avg       0.79      0.78      0.78       188
```

---

## 🔹 MLP (Neural Net) — *Perceptron multicouche (réseau de neurones)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 45.74 %)*

**Accuracy sur test** : `0.7660`

**Cross-validation (CV)** : `0.7802` ± `0.0235`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.54      0.44      0.48        34
           4       0.89      0.89      0.89        74
           6       0.73      0.80      0.76        79
           7       0.00      0.00      0.00         1

    accuracy                           0.77       188
   macro avg       0.54      0.53      0.53       188
weighted avg       0.76      0.77      0.76       188
```

---

## 🔹 Decision Tree — *Arbre de décision (structure hiérarchique de règles)*

**Parti politique prédit gagnant** : `Droite` *(ID: 6, 42.55 %)*

**Accuracy sur test** : `0.8085`

**Cross-validation (CV)** : `0.8084` ± `0.0147`


**Classification report** :

```text
precision    recall  f1-score   support

           2       0.48      0.47      0.48        34
           4       0.99      1.00      0.99        74
           6       0.78      0.78      0.78        79
           7       0.00      0.00      0.00         1

    accuracy                           0.81       188
   macro avg       0.56      0.56      0.56       188
weighted avg       0.80      0.81      0.81       188
```

---


# 🏆 Modèle le plus performant

### ✅ **Gradient Boosting** — *Gradient Boosting (arbre additif séquentiel)*

- **Accuracy** : `0.8670`

- **Cross-validation** : `0.8583` ± `0.0349`

- **Parti politique prédit gagnant** : `Droite` *(ID: 6, 43.62 %)*


### 🎯 Pourquoi ce modèle est le meilleur ?

- Il obtient la meilleure performance en termes d'**accuracy** sur l'ensemble de test.
- Il maintient une **stabilité élevée** avec une faible variance en cross-validation.
- Il prédit de façon cohérente le parti gagnant avec une confiance élevée dans la majorité des départements.