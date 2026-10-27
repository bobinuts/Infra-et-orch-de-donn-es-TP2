-- HealthStream ICU Monitor - schéma d'ingestion (séance 2)
--
-- Exécuté UNE SEULE FOIS, au premier démarrage du volume pg-data, par le mécanisme
-- d'initialisation de l'image Chainguard (répertoire /var/lib/postgres/initdb/).
-- Le script est rejouable (IF NOT EXISTS / OR REPLACE) : on peut aussi l'appliquer à la main
-- avec « docker compose exec -T postgres psql -U hs_user -d healthstream < sql/init.sql ».
--
-- Ces noms (table, colonnes, contraintes, index, vue) sont le contrat d'interface avec la
-- séance 3 (Airflow) et la séance 4 (Flink) : ne pas les modifier.

-- 1. La table : coordonnées Kafka du message + charge utile du format de la séance 1
--    + horodatage d'ingestion posé par la base.
CREATE TABLE IF NOT EXISTS vitals_raw (
    kafka_partition INTEGER       NOT NULL,   -- coordonnée Kafka : identifie le message
    kafka_offset    BIGINT        NOT NULL,   -- coordonnée Kafka : stable à la relecture
    patient_id      VARCHAR(16)   NOT NULL,   -- clé de partitionnement de la séance 1
    event_time      TIMESTAMPTZ   NOT NULL,   -- ms epoch converties par le consommateur
    vital_type      VARCHAR(16)   NOT NULL,   -- HR, SPO2 ou ABP_SYS
    value           NUMERIC(8, 2) NOT NULL,   -- valeur du relevé
    unit            VARCHAR(8)    NOT NULL,   -- unité du relevé
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),   -- latence de bout en bout

    -- Rejouer le même message Kafka ne peut pas créer une seconde ligne.
    CONSTRAINT pk_vitals_raw PRIMARY KEY (kafka_partition, kafka_offset),
    CONSTRAINT ck_vital_type CHECK (vital_type IN ('HR', 'SPO2', 'ABP_SYS'))
);

-- 2. Index de lecture, dimensionné pour la requête dominante :
--    « la série d'un patient sur une fenêtre récente ».
CREATE INDEX IF NOT EXISTS idx_vitals_patient_time
    ON vitals_raw (patient_id, event_time DESC);

-- 3. Vue de contrôle : cinq agrégats que la partie 5 interroge pour prouver
--    l'absence de doublon (lignes_totales = messages_distincts).
CREATE OR REPLACE VIEW v_vitals_controle AS
SELECT COUNT(*)                                        AS lignes_totales,
       COUNT(DISTINCT (kafka_partition, kafka_offset)) AS messages_distincts,
       COUNT(DISTINCT patient_id)                      AS patients_distincts,
       MIN(event_time)                                 AS premier_releve,
       MAX(event_time)                                 AS dernier_releve
FROM vitals_raw;
