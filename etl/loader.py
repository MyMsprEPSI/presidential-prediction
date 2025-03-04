import logging
import mysql.connector
from pyspark.sql import DataFrame

# Configuration du logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataLoader:
    """
    Classe pour charger dynamiquement des DataFrames dans MySQL.
    - Vérifie et crée la table si elle n'existe pas.
    - Charge les données avec gestion automatique des colonnes.
    """

    def __init__(
        self,
        jdbc_url,
        user,
        password,
        database,
        host,
        port,
        driver="com.mysql.cj.jdbc.Driver",
    ):
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.driver = driver

    def load_data(self, df: DataFrame, table_name: str, mode="append"):
        """
        Charge un DataFrame dans MySQL en créant la table si nécessaire.
        :param df: DataFrame PySpark à insérer
        :param table_name: Nom de la table MySQL
        :param mode: Mode d'insertion (append, overwrite)
        """
        if df is None or df.isEmpty():
            logger.error("❌ Le DataFrame est vide ou invalide.")
            return

        # Connexion MySQL pour vérifier/créer la table
        conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        cursor = conn.cursor()

        try:
            # Création de la table si elle n'existe pas
            self._create_table_if_not_exists(cursor, df, table_name)
            conn.commit()

            # Définition des propriétés JDBC
            props = {
                "user": self.user,
                "password": self.password,
                "driver": self.driver,
            }

            # Chargement des données via JDBC
            df.write.jdbc(
                url=self.jdbc_url, table=table_name, mode=mode, properties=props
            )
            logger.info(f"✅ Données chargées dans la table {table_name} avec succès.")

        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement des données : {str(e)}")

        finally:
            cursor.close()
            conn.close()

    def _create_table_if_not_exists(self, cursor, df: DataFrame, table_name: str):
        """
        Crée une table MySQL si elle n'existe pas, basée sur les colonnes du DataFrame.
        :param cursor: Curseur MySQL
        :param df: DataFrame PySpark
        :param table_name: Nom de la table MySQL
        """
        # Récupération des colonnes et types de données
        schema = df.schema
        columns_sql = []

        type_mapping = {
            "IntegerType": "INT",
            "StringType": "VARCHAR(255)",
            "DoubleType": "DOUBLE",
            "FloatType": "FLOAT",
            "BooleanType": "BOOLEAN",
            "DateType": "DATE",
            "TimestampType": "TIMESTAMP",
        }

        for field in schema.fields:
            sql_type = type_mapping.get(field.dataType.simpleString(), "TEXT")
            columns_sql.append(f"`{field.name}` {sql_type}")

        # Construction de la requête SQL
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns_sql)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        try:
            cursor.execute(create_table_query)
            logger.info(f"✅ Table `{table_name}` vérifiée/créée avec succès.")
        except Exception as e:
            logger.error(
                f"❌ Erreur lors de la création de la table {table_name} : {str(e)}"
            )
