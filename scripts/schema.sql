CREATE TABLE sirene (
    siret VARCHAR(14) NOT NULL PRIMARY KEY,
    siren VARCHAR(9) NOT NULL,
    denomination VARCHAR(255),
    naf_code VARCHAR(10),
    date_creation DATE,
    effectif VARCHAR(20),
    adresse VARCHAR(255),
    code_postal VARCHAR(10),
    ville VARCHAR(100),
    UNIQUE KEY uniq_siren_siret (siren, siret)
);

CREATE TABLE comptes_annuels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    siren VARCHAR(9) NOT NULL,
    annee INT,
    ca BIGINT,
    resultat BIGINT,
    capitaux_propres BIGINT,
    total_bilan BIGINT,
    dettes BIGINT,
    effectif_moyen INT,
    date_depot DATE,
    FOREIGN KEY (siren) REFERENCES sirene(siren)
);

CREATE TABLE bodacc_procedures (
    id INT AUTO_INCREMENT PRIMARY KEY,
    siren VARCHAR(9) NOT NULL,
    type_procedure VARCHAR(100),
    date_procedure DATE,
    source VARCHAR(255),
    FOREIGN KEY (siren) REFERENCES sirene(siren)
);

CREATE TABLE macro_regional (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100),
    annee INT,
    taux_chomage DECIMAL(5,2),
    croissance_pib DECIMAL(5,2),
    indice_prix DECIMAL(6,2)
);