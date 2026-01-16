CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS cnesst_lesions (
    id SERIAL PRIMARY KEY,
    annee INTEGER,
    scian VARCHAR(10),
    genre_accident VARCHAR(10),
    nature_lesion VARCHAR(10),
    nombre_lesions INTEGER,
    jours_perdus INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS domain_risks (
    code VARCHAR(20) PRIMARY KEY,
    nom_fr VARCHAR(100),
    nom_en VARCHAR(100),
    niveau_danger INTEGER
);

INSERT INTO domain_risks VALUES
('ELEC', 'Électricité', 'Electrical', 5),
('CHUTE', 'Chutes', 'Falls', 5),
('MACHINE', 'Machines', 'Machinery', 4)
ON CONFLICT DO NOTHING;
