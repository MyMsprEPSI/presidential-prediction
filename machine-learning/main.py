import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.neural_network import MLPClassifier
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Chargement des données
data = pd.read_csv('fusion_data.csv')

# 2. Exploration et préparation des données
print(data.head())
print(data.info())
print(data.describe())

# Visualisation des corrélations
plt.figure(figsize=(12, 8))
sns.heatmap(data.corr(), annot=True, cmap='coolwarm')
plt.title('Matrice de corrélation')
plt.show()

# 3. Préparation des features et target
X = data.drop(['annee', 'code_departement', 'politique'], axis=1)  # Features
y = data['politique']  # Target (parti politique)

# Normalisation des données
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Séparation en train/test
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.3, random_state=42)

# 4. Modèles de machine learning

# a. Régression logistique pour comprendre l'impact de chaque facteur
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X_train, y_train)
y_pred_log = log_reg.predict(X_test)

print("Régression logistique:")
print(classification_report(y_test, y_pred_log))
print("Accuracy:", accuracy_score(y_test, y_pred_log))

# b. Random Forest pour detecter le maximimum de combinaisons
rf = RandomForestClassifier(n_estimators=100, random_state=42)
rf.fit(X_train, y_train)
y_pred_rf = rf.predict(X_test)

print("\nRandom Forest:")
print(classification_report(y_test, y_pred_rf))
print("Accuracy:", accuracy_score(y_test, y_pred_rf))

# c. Gradient Boosting pour optimiser la prediction
gb = GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42)
gb.fit(X_train, y_train)
y_pred_gb = gb.predict(X_test)

print("\nGradient Boosting:")
print(classification_report(y_test, y_pred_gb))
print("Accuracy:", accuracy_score(y_test, y_pred_gb))

# d. Réseau de neurones pour explorer les modeles et combler les modeles qui ne sont pas prioritaires pour le projet
mlp = MLPClassifier(hidden_layer_sizes=(100, 50), max_iter=1000, random_state=42)
mlp.fit(X_train, y_train)
y_pred_mlp = mlp.predict(X_test)

print("\nRéseau de neurones:")
print(classification_report(y_test, y_pred_mlp))
print("Accuracy:", accuracy_score(y_test, y_pred_mlp))

# 5. Optimisation du meilleur modèle (Random Forest par exemple)
param_grid = {
    'n_estimators': [50, 100, 200],
    'max_depth': [None, 10, 20],
    'min_samples_split': [2, 5, 10]
}

grid_search = GridSearchCV(RandomForestClassifier(random_state=42), param_grid, cv=5)
grid_search.fit(X_train, y_train)

best_rf = grid_search.best_estimator_
y_pred_best = best_rf.predict(X_test)

print("\nMeilleur Random Forest après optimisation:")
print(grid_search.best_params_)
print(classification_report(y_test, y_pred_best))
print("Accuracy:", accuracy_score(y_test, y_pred_best))

# 6. Importance des features
feature_importance = best_rf.feature_importances_
features = X.columns
importance_df = pd.DataFrame({'Feature': features, 'Importance': feature_importance})
importance_df = importance_df.sort_values('Importance', ascending=False)

plt.figure(figsize=(10, 6))
sns.barplot(x='Importance', y='Feature', data=importance_df)
plt.title('Importance des features')
plt.show()

# 8. Visualisation des prédictions
# (À adapter selon vos besoins)
years = data['annee'].unique()
avg_politics = data.groupby('annee')['politique'].mean()

plt.figure(figsize=(10, 6))
plt.plot(years, avg_politics, marker='o', label='Historique')
# plt.plot(future_years, future_predictions, marker='o', label='Prédictions')
plt.xlabel('Année')
plt.ylabel('Orientation politique moyenne')
plt.title('Tendance politique au fil des années')
plt.legend()
plt.grid()
plt.show()
