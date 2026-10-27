#!/usr/bin/env python3
"""Producteur de signes vitaux HealthStream (simulateur ICU, séance 1).

Publie dans le topic Kafka un relevé par patient et par tour, au format du projet :
  clé    = patient_id (UTF-8)
  valeur = {"patient_id", "event_time" (ms epoch), "vital_type", "value", "unit"} en JSON
"""
import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

# --- (1) Configuration : tout vient de l'environnement, avec une valeur par défaut -----------
# Défauts utilisables depuis l'hôte (port publié 29092) ; en conteneur Compose fournit kafka:9092.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.environ.get("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.environ.get("NB_PATIENTS", "12"))
INTERVAL_S = float(os.environ.get("INTERVAL_S", "1.0"))  # ~1 Hz, comme un moniteur ICU
# Ajout facultatif pour les mesures automatisées : 0 = tourner jusqu'à SIGINT/SIGTERM.
DUREE_S = float(os.environ.get("DUREE_S", "0"))

# Plages physiologiques : type -> (minimum, maximum, unité)
PLAGES = {
    "HR": (55, 115, "bpm"),
    "SPO2": (90, 100, "%"),
    "ABP_SYS": (95, 145, "mmHg"),
}

arret = False  # drapeau levé par le gestionnaire de signal
stats = {"livres": 0, "echecs": 0}


def gerer_signal(signum, frame):
    """SIGINT / SIGTERM : lève le drapeau d'arrêt, la boucle sort proprement."""
    global arret
    arret = True


def creer_producteur() -> Producer:
    """Construit le producteur avec sa configuration de fiabilité."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "vitals-simulators",
        "acks": "all",                      # leader + ISR : aucun message acquitté ne se perd
        "linger.ms": 20,                    # regroupe les 12 relevés d'un tour en un seul lot
        "retries": 5,                       # réémissions sur erreur transitoire
        "enable.idempotence": True,         # un retry ne crée ni doublon ni inversion d'ordre
        "partitioner": "murmur2_random",    # même hachage que le client Java (Flink, séances 4-6)
    }
    return Producer(conf)


def accuse_reception(err, msg):
    """Callback de livraison, appelé par poll() / flush() : n'affiche que les échecs."""
    if err is not None:
        stats["echecs"] += 1
        print(f"ECHEC livraison (clé={msg.key()}): {err}", file=sys.stderr, flush=True)
    else:
        stats["livres"] += 1


def generer_releve(patient_id: str) -> dict:
    """Un relevé conforme au format 4.1 (event_time en ms epoch, value à 1 décimale)."""
    vital_type = random.choice(list(PLAGES))
    bas, haut, unite = PLAGES[vital_type]
    return {
        "patient_id": patient_id,
        "event_time": int(time.time() * 1000),
        "vital_type": vital_type,
        "value": round(random.uniform(bas, haut), 1),
        "unit": unite,
    }


def main() -> int:
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    producteur = creer_producteur()
    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]
    print(
        f"Producteur démarré : broker={KAFKA_BOOTSTRAP} topic={VITALS_TOPIC} "
        f"patients={NB_PATIENTS} intervalle={INTERVAL_S}s",
        flush=True,
    )

    envoyes = 0
    debut = time.monotonic()
    while not arret:
        if DUREE_S and time.monotonic() - debut >= DUREE_S:
            break
        # --- (3) un relevé par patient et par tour -------------------------------------------
        for patient_id in patients:
            releve = generer_releve(patient_id)
            try:
                producteur.produce(
                    topic=VITALS_TOPIC,
                    key=patient_id.encode("utf-8"),                # clé de partitionnement
                    value=json.dumps(releve).encode("utf-8"),      # valeur : le relevé sérialisé
                    on_delivery=accuse_reception,
                )
                envoyes += 1
            except BufferError:
                # File d'attente locale pleine (broker injoignable) : on laisse le temps de vider.
                print("File d'attente locale pleine, relevé ignoré", file=sys.stderr, flush=True)
                producteur.poll(1)
            producteur.poll(0)  # traite les accusés en attente SANS bloquer la boucle
        time.sleep(INTERVAL_S)

    # --- (4) arrêt propre : flush borné puis bilan ----------------------------------------------
    restants = producteur.flush(10)  # bloque jusqu'à expédition du buffer, 10 s au plus
    duree = time.monotonic() - debut
    debit = envoyes / duree if duree > 0 else 0.0
    print(
        f"Bilan : envoyés={envoyes} livrés={stats['livres']} "
        f"non livrés={restants + stats['echecs']} durée={duree:.1f}s débit={debit:.1f} msg/s",
        flush=True,
    )
    return 0 if restants == 0 and stats["echecs"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
