#!/usr/bin/env python3
"""Consommateur de persistance HealthStream (séance 2, partie 4).

Lit le topic mimic-vitals, accumule les messages dans un lot en mémoire, écrit le lot dans
PostgreSQL en UNE instruction SQL, puis — et seulement alors — acquitte les offsets Kafka.

L'ordre critique : inserer_lot() d'abord, commit() Kafka ensuite. Dans cet ordre, un incident
survenu entre les deux provoque au pire la relecture d'un lot déjà écrit, et la clé primaire
(kafka_partition, kafka_offset) l'absorbe. Dans l'ordre inverse, le même incident provoquerait
une perte définitive : les offsets auraient avancé sur des données qui ne sont nulle part.

Un lot part quand il atteint BATCH_SIZE lignes ou quand BATCH_TIMEOUT_S s'est écoulé depuis le
dernier envoi. Un message malformé est compté puis ignoré, mais tout de même acquitté (sinon le
consommateur bouclerait sur lui). Si la base est indisponible, le lot est conservé, rien n'est
acquitté, la connexion est refaite et le lot repart au tour suivant.
"""
import json
import math
import os
import signal
import socket
import sys
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg2.extras import execute_values

# --- Configuration : tout vient de l'environnement, avec une valeur par défaut -------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.environ.get("VITALS_TOPIC", "mimic-vitals")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "vitals-persister")
AUTO_OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
BATCH_TIMEOUT_S = float(os.environ.get("BATCH_TIMEOUT_S", "5.0"))
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "55432"))
PG_DB = os.environ.get("PG_DB", "healthstream")
PG_USER = os.environ.get("PG_USER", "hs_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "hs_password")
# Ajout facultatif pour les mesures automatisées : 0 = tourner jusqu'à SIGINT/SIGTERM.
DUREE_S = float(os.environ.get("DUREE_S", "0"))

TYPES_VALIDES = ("HR", "SPO2", "ABP_SYS")   # contrainte ck_vital_type

# execute_values remplace %s par tous les tuples du lot : une seule instruction et un seul
# aller-retour réseau. La clause de conflit rend l'insertion idempotente : un message déjà écrit
# (mêmes coordonnées Kafka) est ignoré, la ligne existante est conservée.
SQL_INSERT = (
    "INSERT INTO vitals_raw "
    "(kafka_partition, kafka_offset, patient_id, event_time, vital_type, value, unit) "
    "VALUES %s "
    "ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING"
)

arret = False  # drapeau levé par le gestionnaire de signal


def gerer_signal(signum, frame):
    """SIGINT / SIGTERM : lève le drapeau, la boucle sort d'elle-même et close() est appelé."""
    global arret
    arret = True


def horodatage() -> str:
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + ".%03d" % (int(t * 1000) % 1000)


def creer_consommateur() -> Consumer:
    """Construit le consommateur : acquittement MANUEL, attribution coopérative."""
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "client.id": f"persister-{socket.gethostname()}",   # unique par conteneur, lisible dans --describe
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.auto.commit": False,                        # l'acquittement vient APRÈS l'écriture
        "partition.assignment.strategy": "cooperative-sticky",
        # Doit rester supérieur au temps d'écriture d'un lot, retentatives de connexion comprises.
        "max.poll.interval.ms": 300000,
    })


def connecter_postgres(tentatives: int = 30, delai_s: float = 2.0):
    """Connexion avec retentatives. Indispensable même avec condition: service_healthy, car la
    base peut redevenir indisponible en cours de route (partie 5.3). Patience = tentatives x delai."""
    derniere = None
    for essai in range(1, tentatives + 1):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASSWORD, connect_timeout=5)
            conn.autocommit = False        # transactions explicites : tout le lot ou rien
            if essai > 1:
                print(f"Connexion PostgreSQL retablie (essai {essai}/{tentatives})", flush=True)
            return conn
        except psycopg2.OperationalError as err:
            derniere = err
            motif = str(err).strip().splitlines()[0] if str(err).strip() else repr(err)
            print(f"PostgreSQL indisponible (essai {essai}/{tentatives}): {motif}", flush=True)
            time.sleep(delai_s)
    raise RuntimeError(f"base injoignable apres {tentatives} essais: {derniere}")


def ligne_depuis_message(msg) -> tuple:
    """Message Kafka -> tuple d'insertion. Laisse remonter JSONDecodeError, KeyError, ValueError
    et TypeError sur un message malformé : l'appelant les intercepte pour ne pas faire tomber
    le lot entier. Les valeurs que la base refuserait (type inconnu, champ trop long, nombre
    hors plage) sont rejetées ici en ValueError : un seul message ne doit jamais faire échouer
    la transaction d'un lot de 200."""
    releve = json.loads(msg.value())
    if not isinstance(releve, dict):
        raise TypeError("la valeur JSON n'est pas un objet")
    patient_id = str(releve["patient_id"])
    vital_type = releve["vital_type"]
    unit = str(releve["unit"])
    valeur = float(releve["value"])
    if vital_type not in TYPES_VALIDES:
        raise ValueError(f"vital_type inconnu: {vital_type!r}")
    if not patient_id or len(patient_id) > 16 or not unit or len(unit) > 8:
        raise ValueError("patient_id ou unit hors limites")
    if not math.isfinite(valeur) or abs(valeur) >= 1_000_000:
        raise ValueError(f"value hors plage: {valeur!r}")
    try:
        # event_time arrive en millisecondes epoch : division par 1000 et fuseau UTC
        event_time = datetime.fromtimestamp(int(releve["event_time"]) / 1000, tz=timezone.utc)
    except (OverflowError, OSError) as err:
        raise ValueError(f"event_time hors plage: {err}") from err
    return (msg.partition(), msg.offset(), patient_id, event_time, vital_type, valeur, unit)


def inserer_lot(conn, lignes: list) -> int:
    """Écrit le lot en une instruction et valide la transaction. Retourne le nombre de lignes
    réellement insérées : les doublons absorbés par ON CONFLICT n'y comptent pas."""
    if not lignes:
        return 0
    try:
        with conn.cursor() as cur:
            execute_values(cur, SQL_INSERT, lignes, page_size=len(lignes))
            inserees = cur.rowcount
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    return inserees


def acquitter(consommateur) -> bool:
    """Acquittement synchrone des positions courantes. Appelé seulement APRÈS inserer_lot().
    Un échec ici (rééquilibrage en cours, membre exclu) n'est pas grave : le lot est déjà en
    base, il sera relu et absorbé par la clé primaire."""
    try:
        consommateur.commit(asynchronous=False)    # bloque jusqu'à confirmation du broker
        return True
    except KafkaException as err:
        if err.args and err.args[0].code() == KafkaError._NO_OFFSET:
            return True                            # rien de nouveau à acquitter
        print(f"AVERTISSEMENT commit Kafka impossible ({err}): lot deja en base, "
              f"relecture possible, absorbee par la cle primaire", flush=True)
        return False


def main() -> int:
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    conn = connecter_postgres()
    consommateur = creer_consommateur()

    lot = []                        # tuples prêts à insérer
    rejets_en_attente = False       # un message ignoré attend encore son acquittement
    lus = inseres = ignores = rejets = 0
    debut = dernier_envoi = time.time()

    def sur_attribution(c, partitions):
        liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
        print(f"ATTRIBUTION t={horodatage()} partitions=[{liste}]", flush=True)

    def sur_retrait(c, partitions):
        """Dernier moment pour écrire ce qu'on a en mémoire avant que ces partitions soient
        confiées à un autre membre. Au pire le lot est relu et absorbé par la clé primaire."""
        nonlocal lot, rejets_en_attente, inseres, ignores, dernier_envoi
        liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
        print(f"RETRAIT t={horodatage()} partitions=[{liste}] lot_en_cours={len(lot)}", flush=True)
        if not (lot or rejets_en_attente):
            return
        try:
            n = inserer_lot(conn, lot)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as err:
            print(f"RETRAIT: ecriture impossible ({type(err).__name__}), lot conserve", flush=True)
            return
        acquitter(c)
        inseres += n
        ignores += len(lot) - n
        lot, rejets_en_attente, dernier_envoi = [], False, time.time()

    consommateur.subscribe([VITALS_TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)
    print(f"Consommateur demarre : broker={KAFKA_BOOTSTRAP} topic={VITALS_TOPIC} "
          f"groupe={CONSUMER_GROUP} base={PG_HOST}:{PG_PORT}/{PG_DB} "
          f"BATCH_SIZE={BATCH_SIZE} BATCH_TIMEOUT_S={BATCH_TIMEOUT_S}", flush=True)

    try:
        while not arret:
            if DUREE_S and time.time() - debut >= DUREE_S:
                break
            msg = consommateur.poll(1.0)           # timeout en SECONDES
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"ERREUR Kafka: {msg.error()}", flush=True)
                else:
                    lus += 1
                    try:
                        lot.append(ligne_depuis_message(msg))
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as err:
                        # Malformé : compté, ignoré, mais acquitté avec le lot suivant.
                        rejets += 1
                        rejets_en_attente = True
                        print(f"REJET partition={msg.partition()} offset={msg.offset()} "
                              f"({type(err).__name__}: {err})", flush=True)

            lot_pret = len(lot) >= BATCH_SIZE or (
                (lot or rejets_en_attente) and time.time() - dernier_envoi >= BATCH_TIMEOUT_S)
            if lot_pret:
                debut_sql = time.time()
                try:
                    n = inserer_lot(conn, lot)                # 1. la base d'abord
                    acquitter(consommateur)                   # 2. l'acquittement ENSUITE
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as err:
                    motif = str(err).strip().splitlines()[0] if str(err).strip() else repr(err)
                    print(f"ERREUR PostgreSQL ({type(err).__name__}): {motif} -- lot de "
                          f"{len(lot)} conserve, offsets NON acquittes, reconnexion", flush=True)
                    conn = connecter_postgres()               # lot CONSERVÉ, offsets NON acquittés
                    continue
                inseres += n
                ignores += len(lot) - n
                if lot:
                    print(f"LOT taille={len(lot)} inseres={n} doublons={len(lot) - n} "
                          f"sql_ms={(time.time() - debut_sql) * 1000:.1f}", flush=True)
                lot, rejets_en_attente, dernier_envoi = [], False, time.time()

        # --- Arrêt propre : écrire ce qui reste en mémoire, acquitter, puis quitter le groupe ---
        if lot or rejets_en_attente:
            try:
                n = inserer_lot(conn, lot)
                acquitter(consommateur)
                inseres += n
                ignores += len(lot) - n
                lot, rejets_en_attente = [], False
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as err:
                print(f"ATTENTION base indisponible a l'arret ({type(err).__name__}): lot residuel "
                      f"de {len(lot)} non ecrit et NON acquitte, il sera relu", flush=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        consommateur.close()                               # quitte explicitement le groupe

    duree = time.time() - debut
    debit = lus / duree if duree > 0 else 0.0
    print(f"Arret. lus={lus} inseres={inseres} ignores_doublons={ignores} rejets={rejets} "
          f"duree={duree:.1f}s debit={debit:.1f} msg/s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
