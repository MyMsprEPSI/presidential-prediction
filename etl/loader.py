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

        # Extraire le nom de base du fichier source et générer le nom du fichier final
        base_name = os.path.basename(input_file_path).replace(".csv", "_processed.csv")
        temp_output_dir = os.path.join(
            "data/processed_data", "temp_csv_output"
        )  # Dossier temporaire
        final_output_path = os.path.join("data/processed_data", base_name)

        logger.info(
            f"💾 Enregistrement des données transformées dans : {final_output_path}"
        )

        try:
            # Sauvegarde en CSV dans un dossier temporaire avec une seule partition (1 seul fichier)
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
                temp_output_dir
            )

            # Trouver le fichier CSV généré dans le dossier temporaire
            for filename in os.listdir(temp_output_dir):
                if filename.endswith(".csv"):
                    temp_csv_path = os.path.join(temp_output_dir, filename)
                    shutil.move(
                        temp_csv_path, final_output_path
                    )  # Renommer le fichier final
                    break

            # Supprimer le dossier temporaire
            shutil.rmtree(temp_output_dir)

            logger.info("✅ Fichier CSV sauvegardé avec succès !")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement du fichier : {str(e)}")
