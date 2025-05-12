# Projet de prédictions des Tendances Politiques en France

<img src="./assets/images/logo_predi_presidentielle.png">

<div align="center">

[FR](./README.fr.md) | [EN](README.md)

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.2.0-orange.svg)](https://spark.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-blue.svg)](https://www.mysql.com/)
[![Docker](https://img.shields.io/badge/Docker-20.10-blue.svg)](https://www.docker.com/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.0-orange.svg)](https://scikit-learn.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-2.105-yellow.svg)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-success.svg)]()
[![Contributors](https://img.shields.io/badge/Contributors-4-brightgreen.svg)]()
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation](https://img.shields.io/badge/docs-disponible-brightgreen.svg)]()

</div>

## 📋 Description du Projet

Ce projet vise à analyser les facteurs qui influencent les tendances politiques en France à travers différentes périodes électorales. En utilisant une approche Big Data, nous collectons, transformons et analysons des données provenant de multiples sources pour comprendre les corrélations entre les préférences politiques et divers indicateurs socio-économiques, environnementaux, démographiques et autres.

## 🎯 Objectifs

- Extraire et intégrer des données hétérogènes provenant de différentes sources
- Transformer et normaliser les données pour permettre des analyses croisées
- Développer des modèles prédictifs pour comprendre les facteurs influençant les choix politiques
- Visualiser les résultats à travers des tableaux de bord interactifs

## 🏗️ Structure du Projet

```
├── assets/                 # Ressources graphiques et médias
├── data/                   # Données brutes organisées par catégorie
│   ├── demographie/        # Données démographiques par région
│   ├── education/          # Statistiques sur les établissements éducatifs
│   ├── environnemental/    # Données sur l'énergie renouvelable
│   ├── politique/          # Résultats électoraux historiques
│   ├── sante/              # Indicateurs de santé publique
│   ├── securite/           # Statistiques de criminalité
│   ├── socio-economie/     # PIB, inflation et autres indicateurs économiques
│   ├── technologie/        # Données sur la R&D et l'innovation
│   └── processed_data/     # Données transformées prêtes à être analysées
├── database/               # Scripts SQL et configuration de la base de données
├── docs/                   # Documentation technique et fonctionnelle
│   ├── diagram/            # Diagrammes du projet
│   ├── mcd/                # Modèles conceptuels de données
│   └── schemas/            # Schémas techniques
├── etl/                    # Pipeline d'extraction, transformation et chargement
│   ├── extract.py          # Module d'extraction des données
│   ├── transform.py        # Module de transformation des données
│   └── loader.py           # Module de chargement des données
├── machine-learning/       # Modèles prédictifs et analyses
│   └── main.py             # Application d'apprentissage automatique
├── powerbi/                # Tableaux de bord et visualisations
│   └── datavisualisation_predilection_presidentiel.pbix  # Rapport Power BI
├── .gitignore              # Fichiers ignorés par Git
├── docker-compose.yml      # Configuration des services Docker
├── LICENSE                 # Licence du projet
├── main.py                 # Point d'entrée principal du projet
└── requirements.txt        # Dépendances Python
```

## 🚀 Technologies Utilisées

- **Python** : Langage principal de développement
- **PySpark** : Traitement de données distribuées à grande échelle
- **MySQL** : Stockage relationnel des données transformées
- **Docker** : Containerisation des services
- **Scikit-learn** : Bibliothèque d'apprentissage automatique
- **Power BI** : Visualisation et tableaux de bord

## ⚙️ Installation et Configuration

### Prérequis

- Python 3.8+
- Docker et Docker Compose
- MySQL
- Power BI Desktop (pour visualiser les rapports)

### Configuration de l'environnement

1. **Cloner le dépôt** :

   ```bash
   git clone https://github.com/MyMsprEPSI/bigdata-project.git
   cd bigdata-project
   ```

2. **Créer un environnement virtuel** :

   ```bash
   python -m venv venv
   source venv/bin/activate  # Sur Windows : venv\Scripts\activate
   ```

3. **Installer les dépendances** :

   ```bash
   pip install -r requirements.txt
   ```

4. **Configurer les variables d'environnement** :
   Créer un fichier `.env` à la racine du projet avec les informations suivantes :

   ```
   JDBC_URL=jdbc:mysql://localhost:3306/bigdata
   DB_USER=root
   DB_PASSWORD=
   JDBC_DRIVER=com.mysql.cj.jdbc.Driver
   DB_NAME=bigdata
   ```

5. **Démarrer les services avec Docker** :
   ```bash
   docker-compose up -d
   ```
   Cette commande lance les services MySQL et phpMyAdmin définis dans le fichier docker-compose.yml.

## 📊 Utilisation du Projet

### Pipeline ETL

Le processus ETL (Extract, Transform, Load) gère l'extraction des données brutes, leur transformation et leur chargement dans la base de données.

1. **Extraction des données** :
   Le module `extract.py` récupère les données de différentes sources (fichiers CSV, Excel, etc.) concernant :

   - Résultats électoraux historiques (1965-2022)
   - Données démographiques par région
   - Indicateurs économiques (PIB, inflation)
   - Données environnementales (énergie renouvelable)
   - Statistiques d'éducation, de santé et de sécurité

2. **Transformation des données** :
   Le module `transform.py` nettoie, normalise et prépare les données pour l'analyse :

   - Standardisation des formats de date et de région
   - Calcul d'indicateurs dérivés
   - Fusion de différentes sources de données
   - Traitement des valeurs manquantes

3. **Chargement des données** :
   Le module `loader.py` charge les données transformées dans la base de données MySQL selon un modèle relationnel optimisé.

Pour exécuter le pipeline ETL complet :

```bash
python main.py
```

### Modèles d'Apprentissage Automatique

Le module machine-learning contient les modèles prédictifs qui analysent les corrélations entre les différentes variables et les tendances politiques.

1. **Préparation des données** :

   - Division en ensembles d'entraînement et de test
   - Normalisation des features
   - Sélection des caractéristiques pertinentes

2. **Modèles implémentés** :

   - Régression logistique
   - Forêt aléatoire (Random Forest)
   - SVM (Support Vector Machine)
   - Gradient Boosting
   - KNN (K-Nearest Neighbors)
   - Réseau de neurones (MLP)
   - Ensemble de modèles (Voting Classifier)

3. **Évaluation des performances** :
   - Validation croisée
   - Matrice de confusion
   - Rapports de classification détaillés

Pour entraîner et évaluer les modèles :

```bash
python machine-learning/main.py
```

### Visualisation des Données

Les données transformées et les résultats des modèles sont visualisés à travers des tableaux de bord Power BI.

1. **Ouvrir le rapport** :

   - Lancez Power BI Desktop
   - Ouvrez le fichier `powerbi/datavisualisation_predilection_presidentiel.pbix`

2. **Fonctionnalités du tableau de bord** :
   - Évolution des tendances politiques par région et par année
   - Corrélations entre facteurs socio-économiques et préférences politiques
   - Cartographie des résultats électoraux
   - Prédictions basées sur les modèles d'apprentissage automatique

## 🛠️ Accès à la Base de Données

- **Interface d'administration** : http://localhost:8080 (phpMyAdmin)
- **Identifiants par défaut** :
  - Utilisateur : `root`
  - Mot de passe : ` ` (vide)

## 📊 Modèle de Données

Le projet utilise un modèle en étoile (star schema) pour organiser les données :

- Table de faits principale : `fact_resultats_politique`
- Tables de dimensions :
  - `dim_politique` : partis et étiquettes politiques
  - `dim_securite` : indicateurs de criminalité
  - `dim_socio_economie` : PIB, inflation, etc.
  - `dim_sante` : espérance de vie et autres indicateurs
  - `dim_environnement` : données sur l'énergie renouvelable
  - `dim_education` : statistiques sur l'éducation
  - `dim_demographie` : population par région et par âge
  - `dim_technologie` : dépenses R&D et innovation

## 📉 Analyses Clés

Le projet permet d'explorer plusieurs questions analytiques :

- Comment les facteurs économiques influencent-ils les préférences politiques ?
- Existe-t-il des corrélations entre les investissements environnementaux et les tendances électorales ?
- Quel est l'impact des niveaux d'éducation sur les choix politiques ?
- Comment les indicateurs de santé et de sécurité affectent-ils les résultats électoraux ?

## 🤝 Contribuer au Projet

Les contributions sont les bienvenues ! Voici comment vous pouvez participer :

1. Forkez le projet
2. Créez une branche pour votre fonctionnalité : `git checkout -b nouvelle-fonctionnalite`
3. Committez vos changements : `git commit -m 'Ajout d'une nouvelle fonctionnalité'`
4. Poussez vers la branche : `git push origin nouvelle-fonctionnalite`
5. Ouvrez une Pull Request

## 📜 Licence

Ce projet est distribué sous la licence spécifiée dans le fichier LICENSE.

## 👥 Équipe et Contacts

- [Thomas GARCIA](https://github.com/Keods30)
- [Thibaut MAURRAS](https://github.com/Foufou-exe)
- [Jonathan DELLA SANTINA](https://github.com/JonathanDS30)
- [Joyce Leaticia LAETITIA](https://github.com/JoyceLeaticia)
