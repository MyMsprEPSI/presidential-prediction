# Political Trends Analysis Project

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
[![Documentation](https://img.shields.io/badge/docs-available-brightgreen.svg)]()

</div>

## 📋 Project Description

This project aims to analyze the factors influencing political trends in France across different electoral periods. Using a Big Data approach, we collect, transform, and analyze data from multiple sources to understand the correlations between political preferences and various socio-economic, environmental, demographic, and other indicators.

## 🎯 Objectives

- Extract and integrate heterogeneous data from different sources
- Transform and normalize data to enable cross-analysis
- Develop predictive models to understand factors influencing political choices
- Visualize results through interactive dashboards

## 🏗️ Project Structure

```
├── assets/                 # Graphic resources and media
├── data/                   # Raw data organized by category
│   ├── demographie/        # Demographic data by region
│   ├── education/          # Statistics on educational institutions
│   ├── environnemental/    # Renewable energy data
│   ├── politique/          # Historical electoral results
│   ├── sante/              # Public health indicators
│   ├── securite/           # Crime statistics
│   ├── socio-economie/     # GDP, inflation, and other economic indicators
│   ├── technologie/        # R&D and innovation data
│   └── processed_data/     # Transformed data ready for analysis
├── database/               # SQL scripts and database configuration
├── docs/                   # Technical and functional documentation
│   ├── diagram/            # Project diagrams
│   ├── mcd/                # Conceptual data models
│   └── schemas/            # Technical schemas
├── etl/                    # Extract, Transform, Load pipeline
│   ├── extract.py          # Data extraction module
│   ├── transform.py        # Data transformation module
│   └── loader.py           # Data loading module
├── machine-learning/       # Predictive models and analyses
│   └── main.py             # Machine learning application
├── powerbi/                # Dashboards and visualizations
│   └── datavisualisation_predilection_presidentiel.pbix  # Power BI report
├── .gitignore              # Files ignored by Git
├── docker-compose.yml      # Docker services configuration
├── LICENSE                 # Project license
├── main.py                 # Main project entry point
└── requirements.txt        # Python dependencies
```

## 🚀 Technologies Used

- **Python**: Main development language
- **PySpark**: Distributed large-scale data processing
- **MySQL**: Relational storage for transformed data
- **Docker**: Service containerization
- **Scikit-learn**: Machine learning library
- **Power BI**: Visualization and dashboards

## ⚙️ Installation and Configuration

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- MySQL
- Power BI Desktop (for viewing reports)

### Environment Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/MyMsprEPSI/bigdata-project.git
   cd bigdata-project
   ```

2. **Create a virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   Create a `.env` file at the project root with the following information:

   ```
   JDBC_URL=jdbc:mysql://localhost:3306/bigdata
   DB_USER=root
   DB_PASSWORD=
   JDBC_DRIVER=com.mysql.cj.jdbc.Driver
   DB_NAME=bigdata
   ```

5. **Start services with Docker**:
   ```bash
   docker-compose up -d
   ```
   This command launches the MySQL and phpMyAdmin services defined in the docker-compose.yml file.

## 📊 Project Usage

### ETL Pipeline

The ETL (Extract, Transform, Load) process handles the extraction of raw data, its transformation, and loading into the database.

1. **Data Extraction**:
   The `extract.py` module retrieves data from different sources (CSV files, Excel, etc.) concerning:

   - Historical election results (1965-2022)
   - Demographic data by region
   - Economic indicators (GDP, inflation)
   - Environmental data (renewable energy)
   - Education, health, and security statistics

2. **Data Transformation**:
   The `transform.py` module cleans, normalizes, and prepares the data for analysis:

   - Standardization of date and region formats
   - Calculation of derived indicators
   - Merging different data sources
   - Handling missing values

3. **Data Loading**:
   The `loader.py` module loads the transformed data into the MySQL database according to an optimized relational model.

To execute the complete ETL pipeline:

```bash
python main.py
```

### Machine Learning Models

The machine-learning module contains predictive models that analyze correlations between different variables and political trends.

1. **Data Preparation**:

   - Division into training and test sets
   - Feature normalization
   - Selection of relevant characteristics

2. **Implemented Models**:

   - Logistic Regression
   - Random Forest
   - SVM (Support Vector Machine)
   - Gradient Boosting
   - KNN (K-Nearest Neighbors)
   - Neural Network (MLP)
   - Model Ensemble (Voting Classifier)

3. **Performance Evaluation**:
   - Cross-validation
   - Confusion matrix
   - Detailed classification reports

To train and evaluate the models:

```bash
python machine-learning/main.py
```

### Data Visualization

Transformed data and model results are visualized through Power BI dashboards.

1. **Open the report**:

   - Launch Power BI Desktop
   - Open the file `powerbi/datavisualisation_predilection_presidentiel.pbix`

2. **Dashboard Features**:
   - Evolution of political trends by region and year
   - Correlations between socio-economic factors and political preferences
   - Mapping of electoral results
   - Predictions based on machine learning models

## 🛠️ Database Access

- **Administration Interface**: http://localhost:8080 (phpMyAdmin)
- **Default Credentials**:
  - Username: `root`
  - Password: ` ` (empty)

## 📊 Data Model

The project uses a star schema to organize data:

- Main fact table: `fact_resultats_politique`
- Dimension tables:
  - `dim_politique`: political parties and labels
  - `dim_securite`: crime indicators
  - `dim_socio_economie`: GDP, inflation, etc.
  - `dim_sante`: life expectancy and other indicators
  - `dim_environnement`: renewable energy data
  - `dim_education`: education statistics
  - `dim_demographie`: population by region and age
  - `dim_technologie`: R&D expenditure and innovation

## 📉 Key Analyses

The project allows exploration of several analytical questions:

- How do economic factors influence political preferences?
- Are there correlations between environmental investments and electoral trends?
- What is the impact of education levels on political choices?
- How do health and security indicators affect electoral results?

## 🤝 Contributing to the Project

Contributions are welcome! Here's how you can participate:

1. Fork the project
2. Create a branch for your feature: `git checkout -b new-feature`
3. Commit your changes: `git commit -m 'Add a new feature'`
4. Push to the branch: `git push origin new-feature`
5. Open a Pull Request

## 📜 License

This project is distributed under the license specified in the LICENSE file.

## 👥 Team and Contacts

- [Thomas GARCIA](https://github.com/Keods30)
- [Thibaut MAURRAS](https://github.com/Foufou-exe)
- [Jonathan DELLA SANTINA](https://github.com/JonathanDS30)
- [Joyce Leaticia LAETITIA](https://github.com/JoyceLeaticia)
