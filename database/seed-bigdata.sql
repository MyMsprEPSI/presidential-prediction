CREATE DATABASE IF NOT EXISTS elections_presidentielles;
USE elections_presidentielles;

CREATE TABLE `dim_politique` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `etiquette_parti` INT,
  `annee` INT,
  `code_dept` VARCHAR(3),
  `candidat` VARCHAR(100),
  `total_voix` INT,
  `orientation_politique` VARCHAR(50)
);

CREATE TABLE `dim_securite` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `annee` VARCHAR(10),
  `code_dept` VARCHAR(4),
  `delits_total` INT
);

CREATE TABLE `dim_sante` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `code_dept` VARCHAR(3),
  `annee` INT,
  `esperance_vie` FLOAT
);

CREATE TABLE `dim_education` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `code_departement` VARCHAR(3),
  `annee_fermeture` INT,
  `libelle_departement` VARCHAR(100),
  `nombre_total_etablissements` INT,
  `nb_public` INT,
  `nb_prive` INT,
  `pct_public` FLOAT,
  `pct_prive` FLOAT
);

CREATE TABLE `dim_environnement` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `code_insee_region` VARCHAR(3),
  `annee` INT,
  `parc_eolien_mw` FLOAT,
  `parc_solaire_mw` FLOAT
);

CREATE TABLE `dim_socio_economie` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `annee` INT,
  `pib_euros_par_habitant` FLOAT,
  `code_insee_region` VARCHAR(3),
  `evolution_prix_conso` FLOAT,
  `pib_par_inflation` FLOAT
);

CREATE TABLE `dim_technologie` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `annee` INT,
  `depenses_rd_pib` FLOAT
);

CREATE TABLE `dim_demographie` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `annee` INT,
  `code_departement` VARCHAR(3),
  `nom_departement` VARCHAR(100),
  `population_totale` INT,
  `population_hommes` INT,
  `population_femmes` INT,
  `pop_0_19` INT,
  `pop_20_39` INT,
  `pop_40_59` INT,
  `pop_60_74` INT,
  `pop_75_plus` INT
);

CREATE TABLE `fact_resultats_politique` (
  `annee_code_dpt` VARCHAR(10) PRIMARY KEY,
  `id_parti` INT,
  `securite_id` INT,
  `socio_eco_id` INT,
  `sante_id` INT,
  `environnement_id` INT,
  `education_id` INT,
  `demographie_id` INT,
  `technologie_id` INT
);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`id_parti`) REFERENCES `dim_politique` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`securite_id`) REFERENCES `dim_securite` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`socio_eco_id`) REFERENCES `dim_socio_economie` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`sante_id`) REFERENCES `dim_sante` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`environnement_id`) REFERENCES `dim_environnement` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`education_id`) REFERENCES `dim_education` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`demographie_id`) REFERENCES `dim_demographie` (`id`);

ALTER TABLE `fact_resultats_politique` ADD FOREIGN KEY (`technologie_id`) REFERENCES `dim_technologie` (`id`);
