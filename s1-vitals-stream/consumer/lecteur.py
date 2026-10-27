#!/usr/bin/env python3
"""Lecteur exploratoire HealthStream (séance 2, parties 2 et 3).

Ce script n'écrit nulle part : il lit le topic et affiche. Il sert à observer les offsets
commités, l'acquittement automatique ou manuel et le rééquilibrage d'un consumer group.
Tout est paramétré par l'environnement, pour comparer plusieurs comportements sans toucher
au code. Le consommateur qui persiste est consumer.py (partie 4).

Deux comportements contre-intuitifs, vérifiés en partie 3 :
  - subscribe() n'attribue rien immédiatement : l'attribution arrive au premier poll(), donc la
    ligne ATTRIBUTION ne s'affiche qu'une fois la boucle démarrée ;
  - close() quitte explicitement le groupe, ce qui rend le rééquilibrage des autres membres
    immédiat au lieu d'attendre l'expiration de session.timeout.ms.
"""
import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

# --- (1) Configuration : tout vient de l'environnement, avec une valeur par défaut -------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.environ.get("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.environ.get("CONSUMER_GROUP", "explo-1")
OFFSET_RESET = os.environ.get("AUTO_OFFSET_RESET", "earliest")
AUTO_COMMIT = os.environ.get("AUTO_COMMIT", "true").strip().lower() in ("1", "true", "yes", "oui")
INSTANCE = os.environ.get("INSTANCE", "c1")

# Ajouts facultatifs (valeurs par défaut = comportement de l'énoncé) :
#  - ASSIGNMENT_STRATEGY : range (énoncé 2.3) ou cooperative-sticky (essai de la partie 3.4) ;
#  - DUREE_S : 0 = tourner jusqu'à SIGINT/SIGTERM, sinon sortir proprement après N secondes ;
#  - STOP_FILE : si ce fichier apparaît, sortir proprement (équivalent d'un Ctrl-C, utilisable par
#    un script qui ne peut pas envoyer de Ctrl-C à un processus d'arrière-plan sous Windows).
STRATEGIE = os.environ.get("ASSIGNMENT_STRATEGY", "range")
DUREE_S = float(os.environ.get("DUREE_S", "0"))
STOP_FILE = os.environ.get("STOP_FILE", "")

arret = False       # drapeau levé par le gestionnaire de signal : la boucle sort d'elle-même
lus = 0             # messages valides lus (variable globale lue par les deux rappels)
par_partition = {}  # partition -> nombre de messages lus
invalides = 0       # messages dont la valeur n'est pas du JSON


def horodatage() -> str:
    """Heure locale avec les millisecondes, pour chronométrer les rééquilibrages."""
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + ".%03d" % (int(t * 1000) % 1000)


def gerer_signal(signum, frame):
    """SIGINT / SIGTERM : lève le drapeau d'arrêt, sans quoi close() ne serait jamais appelé."""
    global arret
    arret = True


def creer_consommateur() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,                          # partagé par toutes les instances du groupe
        "client.id": f"lecteur-{INSTANCE}",            # nom de cette instance, visible dans --describe
        "auto.offset.reset": OFFSET_RESET,             # seulement si le groupe n'a aucun offset commité
        "enable.auto.commit": AUTO_COMMIT,             # acquittement périodique ou décision du code
        "partition.assignment.strategy": STRATEGIE,    # range (eager) ou cooperative-sticky
    })


def sur_attribution(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] ATTRIBUTION t={horodatage()} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def sur_retrait(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] RETRAIT t={horodatage()} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def acquitter(consommateur) -> None:
    """Acquittement manuel : bloque jusqu'à confirmation du broker (asynchronous=False)."""
    try:
        consommateur.commit(asynchronous=False)
    except KafkaException as err:
        # _NO_OFFSET : rien de nouveau à acquitter ; autre erreur : on le dit et on continue
        if err.args and err.args[0].code() != KafkaError._NO_OFFSET:
            print(f"[{INSTANCE}] AVERTISSEMENT commit impossible : {err}", flush=True)


def main() -> int:
    global arret, lus, invalides
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    consommateur = creer_consommateur()
    # subscribe() n'attribue rien : l'attribution arrive au premier poll().
    consommateur.subscribe([TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)
    mode = "automatique" if AUTO_COMMIT else "manuel"
    print(f"[{INSTANCE}] DEMARRAGE t={horodatage()} broker={BOOTSTRAP} topic={TOPIC} "
          f"groupe={GROUP_ID} reset={OFFSET_RESET} acquittement={mode} strategie={STRATEGIE}",
          flush=True)

    debut = time.time()
    try:
        while not arret:
            if DUREE_S and time.time() - debut >= DUREE_S:
                break
            if STOP_FILE and os.path.exists(STOP_FILE):
                break
            msg = consommateur.poll(1.0)          # timeout en SECONDES (millisecondes en Java)
            if msg is None:
                continue                          # rien pendant 1 s
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue                      # fin de partition atteinte : pas une erreur
                print(f"[{INSTANCE}] ERREUR {msg.error()}", flush=True)
                continue
            try:
                json.loads(msg.value())           # message valide = JSON décodable
            except (ValueError, TypeError):
                invalides += 1
                continue
            lus += 1
            par_partition[msg.partition()] = par_partition.get(msg.partition(), 0) + 1
            if lus % 50 == 0:                     # une ligne sur cinquante, pour rester lisible
                cle = msg.key().decode("utf-8", "replace") if msg.key() else "-"
                print(f"[{INSTANCE}] lus={lus} partition={msg.partition()} "
                      f"offset={msg.offset()} cle={cle}", flush=True)
            if not AUTO_COMMIT and lus % 100 == 0:
                acquitter(consommateur)           # acquittement manuel tous les 100 messages
    finally:
        # Total lu et répartition, puis close() : quitte explicitement le groupe.
        if not AUTO_COMMIT:
            acquitter(consommateur)               # dernier acquittement d'un arrêt PROPRE
        repartition = dict(sorted(par_partition.items()))
        print(f"[{INSTANCE}] Arret. t={horodatage()} lus={lus} repartition={repartition} "
              f"invalides={invalides}", flush=True)
        consommateur.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
