# loader.py

import logging
import os
import shutil
import pandas as pd
from typing import Optional

# Configuration du logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Classe permettant d'enregistrer un DataFrame PySpark transformé en fichier CSV.
    Le fichier est nommé selon le format "<nom_fichier_base>_processed.csv".
    """

    def __init__(self, output_dir="data\processed_data"):
        """
        Initialise le DataLoader avec un répertoire de sortie.

        :param output_dir: Dossier où seront stockés les fichiers CSV transformés.
        """
        logger.info(
            f"🚀 Initialisation du DataLoader avec le dossier de sortie : {output_dir}"
        )
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas
        logger.info("✅ Dossier de sortie créé/validé")

    def save_to_csv(self, df, input_file_path):
        """
        Sauvegarde un DataFrame en fichier CSV après transformation.
        Compatible avec les DataFrames pandas et Spark.
        """
        if df is None:
            logger.error("❌ Impossible de sauvegarder un DataFrame vide.")
            return

        # Normaliser le chemin d'entrée
        input_file_path = os.path.normpath(input_file_path)
        base_name = os.path.basename(input_file_path).replace(".csv", "_processed.csv")
        final_output_path = os.path.normpath(os.path.join(self.output_dir, base_name))

        logger.info(
            f"⚡ Enregistrement des données transformées dans : {final_output_path}"
        )

        try:
            # Vérifier si c'est un DataFrame pandas ou Spark
            if hasattr(df, "toPandas"):  # C'est un DataFrame Spark
                # Utiliser la méthode originale pour Spark
                df = df.coalesce(1)
                df.write.mode("overwrite").option("header", "true").option(
                    "delimiter", ";"  # Utiliser la virgule comme séparateur
                ).csv(final_output_path + "_temp")

                temp_file = next(
                    (
                        os.path.join(final_output_path + "_temp", filename)
                        for filename in os.listdir(final_output_path + "_temp")
                        if filename.endswith(".csv")
                    ),
                    None,
                )
                if temp_file:
                    shutil.copy2(temp_file, final_output_path)
                    shutil.rmtree(final_output_path + "_temp")
                    logger.info("✅ Fichier CSV sauvegardé avec succès !")
                else:
                    logger.error(
                        "❌ Aucun fichier CSV généré dans le dossier temporaire."
                    )
            else:  # C'est un DataFrame pandas
                # Pour le fichier de sécurité, on utilise toujours le header
                df.to_csv(final_output_path, sep=";", index=False, header=True)
                logger.info("✅ Fichier CSV sauvegardé avec succès via pandas!")

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement du fichier : {str(e)}")
            if os.path.exists(final_output_path + "_temp"):
                shutil.rmtree(final_output_path + "_temp")
