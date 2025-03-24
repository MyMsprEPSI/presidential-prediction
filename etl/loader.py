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

        :param df: DataFrame PySpark transformé
        :param input_file_path: Chemin du fichier source initial
        """
        if df is None:
            logger.error("❌ Impossible de sauvegarder un DataFrame vide.")
            return

        # Normaliser le chemin d'entrée
        input_file_path = os.path.normpath(input_file_path)

        # Créer le nom du fichier de sortie
        base_name = os.path.basename(input_file_path).replace(".csv", "_processed.csv")
        final_output_path = os.path.normpath(os.path.join(self.output_dir, base_name))
        temp_output_path = os.path.normpath(
            os.path.join(self.output_dir, f"{base_name}_temp")
        )

        logger.info(
            f"⚡ Enregistrement des données transformées dans : {final_output_path}"
        )

        try:
            # Utiliser toPandas() pour les petits datasets ou repartition() pour les grands
            if df.count() < 1000000:  # Seuil arbitraire, à ajuster selon vos besoins
                # Méthode pour petits datasets
                df.toPandas().to_csv(final_output_path, index=False)
            else:
                # Méthode pour grands datasets
                df.repartition(1).write.mode("overwrite").option("header", "true").csv(
                    temp_output_path
                )

                # Renommer le fichier généré
                for filename in os.listdir(temp_output_path):
                    if filename.endswith(".csv"):
                        os.rename(
                            os.path.join(temp_output_path, filename), final_output_path
                        )

                # Nettoyer le dossier temporaire
                if os.path.exists(temp_output_path):
                    shutil.rmtree(temp_output_path)

            logger.info("✅ Fichier CSV sauvegardé avec succès !")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement du fichier : {str(e)}")
            # Nettoyer le dossier temporaire en cas d'erreur
            if os.path.exists(temp_output_path):
                shutil.rmtree(temp_output_path)
