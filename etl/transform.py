# transform.py

import logging
from pyspark.sql.functions import (
    col,
    when,
    lit,
    isnan,
    sum as spark_sum,
    round,
    regexp_replace,
    concat,
    desc,
    row_number,
    lpad,
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F , types as T
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


# Configuration du logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataTransformer:
    """
    Classe permettant de transformer les données extraites avant leur chargement.
    """

    def __init__(self):
        pass

    def transform_environmental_data(self, df_env):
        """
        Transforme les données environnementales :
        - Sélectionne uniquement les colonnes nécessaires
        - Remplace les valeurs vides par 0.0 pour les valeurs manquantes d'éolien et solaire
        - Ajoute les données de l'année 2000 pour chaque région avec des valeurs à 0.0
        - Regroupe les données par Code_INSEE_région et Année (somme en cas de doublons)
        - Trie les résultats par région et année
        """

        if df_env is None:
            logger.error("❌ Le DataFrame environnemental est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données environnementales en cours...")

        # Sélection des colonnes nécessaires et cast des valeurs
        df_transformed = df_env.select(
            col("Année").cast("int"),
            col("Code_INSEE_Région"),
            col("Parc_installé_éolien_MW").cast("double"),
            col("Parc_installé_solaire_MW").cast("double"),
        )

        # Remplacement des valeurs nulles par 0.0
        df_transformed = df_transformed.fillna(
            {"Parc_installé_éolien_MW": 0.0, "Parc_installé_solaire_MW": 0.0}
        )

        # Regroupement par région et année pour sommer les valeurs en cas de doublons
        df_grouped = df_transformed.groupBy("Code_INSEE_Région", "Année").agg(
            spark_sum("Parc_installé_éolien_MW").alias("Parc_installé_éolien_MW"),
            spark_sum("Parc_installé_solaire_MW").alias("Parc_installé_solaire_MW"),
        )

        # Récupération des régions uniques présentes dans les données
        regions = df_grouped.select("Code_INSEE_Région").distinct()

        # Création d'un DataFrame contenant l'année 2000 pour chaque région avec valeurs à 0.0
        df_year_2000 = (
            regions.withColumn("Année", lit(2000))
            .withColumn("Parc_installé_éolien_MW", lit(0.0))
            .withColumn("Parc_installé_solaire_MW", lit(0.0))
        )

        # Ajout des données de l'année 2000 au DataFrame principal
        df_final = df_grouped.union(df_year_2000)

        # Tri des données par région et année
        df_final = df_final.orderBy("Code_INSEE_Région", "Année")

        logger.info("✅ Transformation terminée ! Aperçu des données transformées :")
        df_final.show(15, truncate=False)

        return df_final

    def transform_pib_outre_mer(self, df_pib, region_codes):
        """
        Transforme les données PIB outre-mer :
        - Suppression des lignes inutiles
        - Ajout du code région INSEE à partir du nom du fichier
        - Tri par Région puis Année
        """

        if df_pib is None:
            logger.error("❌ Le DataFrame PIB est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données PIB outre-mer en cours...")

        # Nettoyage des données
        df_cleaned = df_pib.filter(
            (~col("Année").isin(["idBank", "Dernière mise à jour", "Période"]))
            & (col("Année").rlike("^[0-9]{4}$"))
        ).select(
            col("Année").cast("int"),
            col("PIB_en_euros_par_habitant").cast("int"),
            col("source_file"),
        )

        # Ajout du code région INSEE depuis le dictionnaire region_codes
        condition = None
        for file_path, code_region in region_codes.items():
            if condition is None:
                condition = when(col("source_file") == file_path, lit(code_region))
            else:
                condition = condition.when(
                    col("source_file") == file_path, lit(code_region)
                )

        df_final = df_cleaned.withColumn("Code_INSEE_Région", lit(None))
        for file_path, code_region in region_codes.items():
            df_final = df_final.withColumn(
                "Code_INSEE_Région",
                when(col("source_file") == file_path, lit(code_region)).otherwise(
                    col("Code_INSEE_Région")
                ),
            )

        df_final = df_final.drop("source_file")

        # Tri final
        df_final = df_final.orderBy(["Code_INSEE_Région", "Année"])

        logger.info(
            "✅ Transformation PIB terminée ! Aperçu des données transformées :"
        )
        df_final.show(10, truncate=False)

        return df_final

    def fill_missing_pib_mayotte(self, df_pib):
        """
        Remplit les valeurs manquantes du PIB de Mayotte par régression linéaire.
        """

        from pyspark.sql.functions import col

        logger.info("🚀 Remplissage des valeurs manquantes PIB Mayotte en cours...")

        df_mayotte = df_pib.filter(col("Code_INSEE_Région") == "06")

        known_data = df_mayotte.filter(col("PIB_en_euros_par_habitant").isNotNull())
        unknown_data = df_mayotte.filter(col("PIB_en_euros_par_habitant").isNull())

        assembler = VectorAssembler(inputCols=["Année"], outputCol="features")
        train_data = assembler.transform(known_data).select(
            "features", "PIB_en_euros_par_habitant"
        )

        # Modèle de régression linéaire
        lr = LinearRegression(
            featuresCol="features", labelCol="PIB_en_euros_par_habitant"
        )
        model = lr.fit(train_data)

        # Prédictions sur les données manquantes
        pred_df = assembler.transform(unknown_data)
        pred_result = model.transform(pred_df).select(
            "Année",
            col("prediction").cast("int").alias("PIB_en_euros_par_habitant"),
            "Code_INSEE_Région",
        )

        # Combine les données connues et prédites
        df_mayotte_completed = known_data.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        ).union(pred_result)

        # Autres régions sans modifications
        df_other_regions = df_pib.filter(col("Code_INSEE_Région") != "06")

        # Union finale
        df_final = df_other_regions.union(df_mayotte_completed).orderBy(
            ["Code_INSEE_Région", "Année"]
        )

        logger.info("✅ Remplissage PIB Mayotte terminé :")
        df_final.show(10, truncate=False)

        return df_final

    def combine_all_pib_data(self, df_pib_outremer, df_pib_xlsx, df_pib_2022):
        """
        Combine les données PIB des différentes sources en un seul DataFrame.
        """

        logger.info("🚀 Fusion des données PIB (Outre-mer, Excel, 2022)...")

        # Harmoniser les colonnes
        df_pib_xlsx = df_pib_xlsx.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        )
        df_pib_2022 = df_pib_2022.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        )
        df_pib_outremer = df_pib_outremer.select(
            "Année", "PIB_en_euros_par_habitant", "Code_INSEE_Région"
        )

        # Liste des régions présentes en 2022
        regions_2022 = [
            row["Code_INSEE_Région"]
            for row in df_pib_2022.select("Code_INSEE_Région").distinct().collect()
        ]

        # Identifier les régions absentes en 2022
        missing_regions = (
            df_pib_xlsx.select("Code_INSEE_Région")
            .distinct()
            .filter(~col("Code_INSEE_Région").isin(regions_2022))
        )

        # Ajouter des lignes vides pour les régions absentes en 2022
        if missing_regions.count() > 0:
            df_missing_2022 = missing_regions.withColumn("Année", lit(2022)).withColumn(
                "PIB_en_euros_par_habitant", lit(None).cast("int")
            )
            df_pib_2022 = df_pib_2022.union(df_missing_2022)

        # Fusion des données
        df_final = df_pib_outremer.union(df_pib_xlsx).union(df_pib_2022)

        # **Filtrer les lignes invalides** (Code région doit être numérique et PIB non NULL)
        df_final = df_final.filter(
            (col("Code_INSEE_Région").rlike("^[0-9]+$"))
            & (col("PIB_en_euros_par_habitant").isNotNull())
        )

        # Filtrer et trier
        df_final = df_final.filter((col("Année") >= 2000) & (col("Année") <= 2022))
        df_final = df_final.orderBy(["Code_INSEE_Région", "Année"])

        logger.info("✅ Fusion des données PIB réussie :")
        df_final.show(10, truncate=False)

        return df_final

    def transform_inflation_data(self, df_inflation):
        """
        Transforme les données d'inflation en filtrant les années et en les triant.

        :param df_inflation: DataFrame PySpark contenant les données brutes d'inflation.
        :return: DataFrame PySpark nettoyé et trié.
        """
        if df_inflation is None:
            logger.error("❌ Le DataFrame inflation est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données d'inflation en cours...")

        # Filtrer et trier les données
        df_transformed = df_inflation.orderBy("Année")

        logger.info("✅ Transformation des données d'inflation réussie :")
        df_transformed.show(10, truncate=False)

        return df_transformed

    def combine_pib_and_inflation(self, df_pib, df_inflation):
        """
        Combine les données PIB et Inflation, et calcule le ratio PIB_par_inflation avec arrondi à 2 décimales.

        :param df_pib: DataFrame PySpark contenant le PIB par région.
        :param df_inflation: DataFrame PySpark contenant l'inflation nationale.
        :return: DataFrame PySpark combiné avec le calcul du PIB ajusté par l'inflation.
        """
        if df_pib is None or df_inflation is None:
            logger.error("❌ L'un des DataFrames est vide. Impossible de les combiner.")
            return None

        logger.info("🚀 Fusion des données PIB et Inflation...")

        # Joindre PIB et Inflation sur la colonne Année
        df_combined = df_pib.join(df_inflation, "Année", "left")

        # Utiliser le bon nom de colonne pour l'inflation et arrondir à 2 décimales
        df_combined = df_combined.withColumn(
            "Évolution_des_prix_à_la_consommation",
            round(col("Évolution_des_prix_à_la_consommation"), 2),
        )

        df_combined = df_combined.withColumn(
            "PIB_par_inflation",
            round(
                col("PIB_en_euros_par_habitant")
                / (1 + col("Évolution_des_prix_à_la_consommation") / 100),
                2,
            ),
        )

        # Trier les résultats
        df_combined = df_combined.orderBy("Code_INSEE_Région", "Année")

        logger.info("✅ Fusion des données PIB et Inflation réussie :")
        df_combined.show(10, truncate=False)

        return df_combined

    def transform_technologie_data(self, df):
        """
        Transforme les données de technologie :
        - Renomme les colonnes
        - Convertit les types
        - Nettoie les valeurs d'années
        - Arrondit les pourcentages

        :param df: DataFrame PySpark brut
        :return: DataFrame PySpark transformé
        """
        if df is None:
            logger.error("❌ Le DataFrame technologie est vide ou invalide.")
            return None

        logger.info("🚀 Transformation des données de technologie en cours...")

        try:
            # Sélection et renommage des colonnes
            df_transformed = df.select(
                col("_c0").alias("annee").cast("string"),
                col("DIRD/PIB  France")
                .alias("dird_pib_france_pourcentages")
                .cast("float"),
            )

            # Arrondir les pourcentages à 2 décimales
            df_transformed = df_transformed.withColumn(
                "dird_pib_france_pourcentages",
                round(col("dird_pib_france_pourcentages"), 2),
            )

            # Nettoyer les années
            df_transformed = df_transformed.withColumn(
                "annee", regexp_replace("annee", "\.0", "")
            )

            # Remplacer NaN par 2023
            df_transformed = df_transformed.withColumn(
                "annee", when(col("annee") == "NaN", "2023").otherwise(col("annee"))
            )

            logger.info("✅ Transformation des données de technologie réussie")
            return df_transformed

        except Exception as e:
            logger.error(f"❌ Erreur lors de la transformation des données : {str(e)}")
            return None

    def transform_election_data_1965_2012(self, list_df):
        """
        Transforme et agrège les fichiers CSV 1965-2012.
        - Unpivot via STACK
        - Agrégation par candidat
        - Sélection du candidat gagnant par département et année
        - Nettoyage du nom du candidat
        :param list_df: liste de DataFrames bruts
        :return: DataFrame PySpark final (année, code_dept, candidat, total_voix)
        """
        if not list_df:
            logger.warning("Liste de DataFrames 1965-2012 vide.")
            return None
        results = []
        # Boucle sur chacun des DF (un par fichier CSV)
        for df in list_df:
            # Colonnes clés (à ne pas unpivoter)
            key_columns = {
                "Code département", "Code département0", "Code département1",
                "département", "circonscription", "Inscrits", "Votants",
                "Exprimés", "Blancs et nuls", "filename", "annee"
            }

            # Détermine la bonne colonne de département
            if "Code département" in df.columns:
                dept_col = "Code département"
            elif "Code département0" in df.columns:
                dept_col = "Code département0"
            else:
                # Pas de colonne attendue
                continue

            # Colonnes candidats
            candidate_columns = [c for c in df.columns if c not in key_columns]
            n = len(candidate_columns)
            if n == 0:
                continue

            # Expression stack pour unpivot
            expr_parts = []
            for c in candidate_columns:
                escaped_col = c.replace("'", "''")
                expr_parts.append(f"'{escaped_col}', cast(`{c}` as int)")
            expr = f"stack({n}, {', '.join(expr_parts)}) as (candidat, voix)"

            # Unpivot
            df_unpivot = df.select(
                "annee",
                F.col(dept_col).alias("code_dept"),
                F.expr(expr)
            )

            # Agrégation par département / candidat / année
            df_agg = df_unpivot.groupBy("annee", "code_dept", "candidat") \
                               .agg(F.sum("voix").alias("total_voix"))

            # Sélection du gagnant par dept + année
            windowSpec = Window.partitionBy("annee", "code_dept").orderBy(F.desc("total_voix"))
            df_winner = df_agg.withColumn("rank", F.row_number().over(windowSpec)) \
                              .filter(F.col("rank") == 1)

            # Nettoyage du nom du candidat
            df_winner = df_winner.withColumn("gagnant", F.col("candidat"))
            df_winner = df_winner.withColumn(
                "gagnant",
                F.trim(F.regexp_replace(F.col("gagnant"), r"\([^)]*\)", ""))
            )
            df_winner = df_winner.withColumn(
                "gagnant",
                when(F.col("gagnant") == "SARKOZY", "Nicolas SARKOZY")
                .when(F.col("gagnant") == "CHIRAC", "Jacques CHIRAC")
                .when(F.col("gagnant") == "MITTERRAND", "François MITTERRAND")
                .when(F.col("gagnant") == "DE GAULLE", "Charles DE GAULLE")
                .when(F.col("gagnant") == "GISCARD DESTAING", "Valéry GISCARD DESTAING")
                .when(F.col("gagnant") == "POMPIDOU", "Georges POMPIDOU")
                .when(F.col("gagnant") == "POHER", "Alain POHER")
                .when(F.col("gagnant") == "JOSPIN", "Lionel JOSPIN")
                .when(F.col("gagnant") == "ROYAL", "Ségolène ROYAL")
                .when(F.col("gagnant") == "HOLLANDE", "François HOLLANDE")
                .when(F.col("gagnant") == "MACRON", "Emmanuel MACRON")
                .when(F.col("gagnant") == "LE PEN", "Marine LE PEN")
                .otherwise(F.col("gagnant"))
            )

            # Sélection colonnes finales
            results.append(df_winner.select(
                "annee",
                "code_dept",
                F.col("gagnant").alias("candidat"),
                "total_voix"
            ))

        # Union de tous les résultats
        if results:
            final_df = results[0]
            for df_r in results[1:]:
                final_df = final_df.union(df_r)

            # Normalisation du code_dept (ex: passer '1' -> '01')
            final_df = final_df.withColumn(
                "code_dept_norm",
                F.when(F.col("code_dept").rlike("^[0-9]$"), F.lpad(F.col("code_dept"), 2, "0"))
                 .otherwise(F.col("code_dept"))
            ).drop("code_dept") \
             .withColumnRenamed("code_dept_norm", "code_dept")

            return final_df

        else:
            logger.warning("Aucune donnée agrégée pour 1965-2012.")
            return None

    def transform_election_data_2017(self, df_2017_raw):
        """
        Transforme le fichier Excel 2017 :
        - Sélection du candidat gagnant par département
        - Nettoyage (codes spéciaux pour régions d’outre-mer)
        """
        if df_2017_raw is None:
            logger.warning("DataFrame 2017 vide.")
            return None

        df_2017 = df_2017_raw.withColumnRenamed("Code du département", "code_dept") \
            .withColumn("candidat1", F.concat(F.col("Nom17"), F.lit(" "), F.col("Prénom18"))) \
            .withColumn("candidat2", F.concat(F.col("Nom23"), F.lit(" "), F.col("Prénom24"))) \
            .select(F.col("code_dept").cast("string"),
                    F.col("Libellé du département"),
                    F.col("Voix19").alias("voix1").cast("int"), 
                    F.col("Voix25").alias("voix2").cast("int"),
                    "candidat1", "candidat2"
            )


        # On crée un DataFrame par candidat
        df_2017_candidate1 = df_2017.select("code_dept", 
                                            F.col("candidat1").alias("candidat"), 
                                            F.col("voix1").alias("voix"),
                                            F.col("Libellé du département")
                                            )


        df_2017_candidate2 = df_2017.select("code_dept", 
                                            F.col("candidat2").alias("candidat"), 
                                            F.col("voix2").alias("voix"),
                                            F.col("Libellé du département")
                                            )


        # Union des deux candidats
        df_2017_norm = df_2017_candidate1.union(df_2017_candidate2) \
                            .withColumn("annee", F.lit("2017"))


        # 1. Appliquer le mapping pour les codes spéciaux et la Corse
        df_2017_norm = df_2017_norm.withColumn(
            "code_dept_norm",
            F.when(F.col("Libellé du département") == "Guadeloupe", "ZA")
            .when(F.col("Libellé du département") == "Martinique", "ZB")
            .when(F.col("Libellé du département") == "Guyane", "ZC")
            .when(F.col("Libellé du département") == "La Réunion", "ZD")
            .when(F.col("Libellé du département") == "Mayotte", "ZM")
            .when(F.col("Libellé du département") == "Nouvelle-Calédonie", "ZN")
            .when(F.col("Libellé du département") == "Polynésie française", "ZP")
            .when(F.col("Libellé du département") == "Saint-Pierre-et-Miquelon", "ZS")
            .when(F.col("Libellé du département") == "Saint-Martin/Saint-Barthélemy", "ZX")
            .when(F.col("Libellé du département") == "Wallis et Futuna", "ZW")
            .when(F.col("Libellé du département") == "Français établis hors de France", "ZZ")
            .when(F.col("Libellé du département") == "Corse-du-Sud", "2A")
            .when(F.col("Libellé du département") == "Haute-Corse", "2B")
            .otherwise(F.col("code_dept"))
        )

        # 1. Supprimer la terminaison ".0" dans la colonne "code_dept_norm"
        df_final_2017 = df_2017_norm.withColumn(
            "code_dept_final",
            F.regexp_replace(F.col("code_dept_norm"), r"\.0$", "")
        )

        # 2. (Optionnel) Si vous souhaitez que les codes sur un seul chiffre soient affichés sur 2 chiffres (ex. "1" -> "01")
        df_final_2017 = df_final_2017.withColumn(
            "code_dept_final",
            F.when(F.col("code_dept_final").rlike("^[0-9]$"),
                F.lpad(F.col("code_dept_final"), 2, "0"))
            .otherwise(F.col("code_dept_final"))
        )

        # 3. Supprimer les colonnes intermédiaires et renommer la colonne finale en "code_dept"
        df_final_2017 = df_final_2017.drop("code_dept", "code_dept_norm", "Libellé du département") \
                        .withColumnRenamed("code_dept_final", "code_dept")



        # Pour chaque département, on garde le candidat avec le maximum de voix
        w_dept = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
        df_2017_final = df_final_2017.withColumn("rank", F.row_number().over(w_dept)) \
                            .filter(F.col("rank") == 1) \
                            .select("annee", "code_dept", "candidat", "voix")



        return df_2017_final

    def transform_election_data_2022(self, df_2022_raw):
        """
        Transforme le fichier Excel 2022 :
        - Sélection du gagnant par département
        - Normalisation du nom du candidat (Emmanuel MACRON, Marine LE PEN, etc.)
        """
        if df_2022_raw is None:
            logger.warning("DataFrame 2022 vide.")
            return None

        df_2022 = df_2022_raw.withColumnRenamed("Code du département", "code_dept") \
            .withColumn("candidat", F.concat(F.col("Nom"), F.lit(" "), F.col("Prénom"))) \
            .select(
                F.col("code_dept").cast("string"),
                "candidat",
                F.col("Voix").alias("voix").cast("int")
            ) \
            .withColumn("annee", F.lit("2022"))

        # On agrège par département pour sélectionner le candidat gagnant (le plus de voix)
        w_dept_2022 = Window.partitionBy("annee", "code_dept").orderBy(F.desc("voix"))
        df_2022_final = df_2022.withColumn("rank", F.row_number().over(w_dept_2022)) \
            .filter(F.col("rank") == 1) \
            .select("annee", "code_dept", "candidat", "voix")


        # Normalisation (ex: "MACRON Emmanuel" -> "Emmanuel MACRON")
        df_2022_final = df_2022_final.withColumn(
            "candidat",
            F.when(F.col("candidat") == "MACRON Emmanuel", "Emmanuel MACRON")
             .when(F.col("candidat") == "LE PEN Marine", "Marine LE PEN")
             .otherwise(F.col("candidat"))
        )

        return df_2022_final

    def combine_all_years(self, df_1965_2012, df_2017, df_2022):
        """
        Combine les DataFrames de 1965-2012, 2017 et 2022.
        Applique les mappings DOM-TOM (ZA->971 etc.) et renomme la colonne 'voix' en 'total_voix'.
        """
        if df_1965_2012 is None and df_2017 is None and df_2022 is None:
            logger.warning("Aucun DataFrame à combiner.")
            return None

        import pyspark.sql.functions as F

        # Commence avec les DF non-nuls
        dfs = []
        if df_1965_2012 is not None:
            dfs.append(df_1965_2012)
        if df_2017 is not None:
            dfs.append(df_2017)
        if df_2022 is not None:
            dfs.append(df_2022)

        # Union
        df_final = dfs[0]
        for i in range(1, len(dfs)):
            df_final = df_final.union(dfs[i])

        # Mapping final des codes DOM-TOM
        df_final = df_final.withColumn(
            "code_dept",
            F.when(F.col("code_dept") == "ZA", "971")
             .when(F.col("code_dept") == "ZB", "972")
             .when(F.col("code_dept") == "ZC", "973")
             .when(F.col("code_dept") == "ZD", "974")
             .when(F.col("code_dept") == "ZM", "976")
             .when(F.col("code_dept") == "ZN", "988")
             .when(F.col("code_dept") == "ZP", "987")
             .when(F.col("code_dept") == "ZS", "975")
             .when(F.col("code_dept") == "ZX", "971")  # Saint-Martin/Saint-Barthélemy
             .when(F.col("code_dept") == "ZW", "986")
             .when(F.col("code_dept") == "ZZ", "99")
             .otherwise(F.col("code_dept"))
        )

        # Juste s'assurer du type int pour total_voix
        df_final = df_final.withColumn("total_voix", F.col("total_voix").cast("int"))

        # Tri final
        df_final = df_final.orderBy("annee", "code_dept")

        return df_final