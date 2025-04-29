FROM python:3.12.10 as step-base

# Définition du répertoire de travail
WORKDIR /prediction-presidential-machine

# Installation des dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copie des fichiers de dépendances
COPY requirements.txt .

# Installation des dépendances Python
RUN pip install --no-cache-dir -r requirements.txt


FROM step-base as step-build
# Copie du code source
COPY --from=step-base . /predict-president-etl

# Commande par défaut
CMD ["python", "main.py"]
