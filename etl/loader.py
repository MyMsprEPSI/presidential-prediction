# loader.py

import logging
import os
import shutil

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas

    def save_to_csv(self, df, input_file_path):
        """
        Sauvegarde un DataFrame en fichier CSV après transformation.

        :param df: DataFrame PySpark transformé
        :param input_file_path: Chemin du fichier source initial
        """
        if df is None:
            logger.error("❌ Impossible de sauvegarder un DataFrame vide.")
            return

        base_name = os.path.basename(input_file_path).replace(".csv", "_processed.csv")
        final_output_path = os.path.join("data/processed_data", base_name)

        logger.info(
            f"💾 Enregistrement des données transformées dans : {final_output_path}"
        )

        try:
            # Utiliser toPandas() pour les petits datasets ou repartition() pour les grands
            if df.count() < 1000000:  # Seuil arbitraire, à ajuster selon vos besoins
                # Méthode pour petits datasets
                df.toPandas().to_csv(final_output_path, index=False)
            else:
                # Méthode pour grands datasets
                df.repartition(1).write.mode("overwrite").option("header", "true").csv(
                    final_output_path + "_temp"
                )

                # Renommer le fichier généré
                for filename in os.listdir(final_output_path + "_temp"):
                    if filename.endswith(".csv"):
                        os.rename(
                            os.path.join(final_output_path + "_temp", filename),
                            final_output_path,
                        )

            # Nettoyer le dossier temporaire
            shutil.rmtree(final_output_path + "_temp")

            logger.info("✅ Fichier CSV sauvegardé avec succès !")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement du fichier : {str(e)}")
