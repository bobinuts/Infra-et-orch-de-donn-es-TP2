# Mesures — séance 2

_Étudiant : PELLICANO Théo — Master 2 YNOV Campus Montpellier — Séance 2 : ingestion et persistance (consommateur, consumer groups, PostgreSQL)_

Toutes les valeurs ci-dessous proviennent d'une exécution réelle le 27/10/2026 sur Docker Desktop (Docker 29.8.2, Compose v5.5.1, Python 3.12.7 pour le `.venv`, `confluent-kafka` 2.6.1, `psycopg2-binary` 2.9.10). Les sorties complètes (commandes, résultats et journaux bruts des lecteurs) ont été conservées hors du rendu.

## Décisions retenues

| Décision | Valeur | Raison |
|---|---|---|
| `NB_PARTITIONS` (1.3) | **6** | divisible par 2 et par 3 : 2 consommateurs en prennent 3 chacun, 3 consommateurs en prennent 2 chacun, sans qu'aucun reste inactif (en partie 3.2, 7 instances en laissent une sans partition) |
| `auto.offset.reset` | **`earliest`** | un groupe neuf (ou dont l'offset a expiré) repart du plus ancien message disponible : on ne perd pas l'historique conservé par Kafka ; les rejeux éventuels sont absorbés par la clé primaire |
| Mode d'acquittement de `consumer.py` | **manuel** (`enable.auto.commit: False`) | `commit` seulement après l'écriture du lot en base |
| Clé primaire | **`(kafka_partition, kafka_offset)`** | identifie un message Kafka de façon unique et stable |
| Clause de conflit | **`ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING`** | un message rejoué ne crée pas de seconde ligne |
| Stratégie d'attribution (code de rendu) | **`cooperative-sticky`** dans `consumer/consumer.py` ; `consumer/lecteur.py` garde `range` par défaut, surchargeable par la variable `ASSIGNMENT_STRATEGY` | voir question 3.4 |
| `INTERVAL_S` en 5.2 | **0.005** (avec `BATCH_SIZE=1`) | 12 relevés par tour toutes les 5 ms : débit théorique 2 400 msg/s, très au-dessus de 1 000 msg/s |

### Adaptations à mon environnement (séance 1 conservée)

- Le broker est `confluentinc/cp-kafka:7.8.0` : les outils s'appellent `kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets`, `kafka-console-consumer` (sans `/opt/kafka/bin/…​.sh`), comme en séance 1.
- L'image du consommateur est `python:3.12-slim` (même base que mon producteur de la séance 1) et non `wolfi-base`.
- Poste Windows : les relevés ont été pilotés par des scripts. `Ctrl-C` est remplacé par un arrêt programmé du lecteur (variables facultatives `DUREE_S` et `STOP_FILE` de `lecteur.py`, qui suivent le même chemin que le signal : drapeau levé, sortie de boucle, `commit` final, `close()`), et `kill -9` / `Ctrl-Z` puis `kill -9` par `taskkill /F /T` (ni `close()`, ni départ annoncé, plus aucun heartbeat).
- Le topic `mimic-vitals` n'existait plus (volume `kafka-data` absent) : il a été recréé avec 3 partitions, comme en séance 1.

## Vérification des prérequis

| Contrôle | Valeur relevée |
|---|---|
| Sortie de `docker compose config --quiet` | _(vide)_ : « compose OK » |
| Le volume `kafka-data` existe-t-il encore ? | **non** (aucun volume nommé `kafka-data`) |
| Version de PostgreSQL installée nativement | **aucune** |

## Mesures

### Mesure 1 — État de départ

| Grandeur | Valeur relevée |
|---|---|
| Nombre de partitions de `mimic-vitals` avant modification | **3** |
| Somme des offsets de fin (`kafka-get-offsets --time -1`) | **0** (`mimic-vitals:0:0`, `1:0`, `2:0` : topic recréé, vide) |
| (délai `up -d kafka` → healthy) | 16,8 s |

### Mesure 2 — Répartition observée avant modification

| Partition | Patients observés | Nombre de patients |
|---|---|---|
| 0 | P003, P005, P008, P009, P010, P012 | 6 |
| 1 | P002, P006, P007, P011 | 4 |
| 2 | P001, P004 | 2 |

Cette répartition est identique à celle relevée en séance 1 (P001 → 2, P002 → 1, P003 → 0, …). Le calcul `murmur2(clé) & 0x7fffffff` modulo 3 reproduit exactement ce relevé.

### Mesure 3 — Répartition observée après modification

Topic passé à **6 partitions** (`--alter --partitions 6`), toutes avec `Leader: 1`. Offsets de fin juste après la modification : `0:90`, `1:60`, `2:30`, `3:0`, `4:0`, `5:0` ; relecture de chaque partition à partir de ces bornes.

| Partition | Patients observés (messages récents) | Nombre de patients |
|---|---|---|
| 0 | P005, P010, P012 | 3 |
| 1 | P007 | 1 |
| 2 | _aucun_ | 0 |
| 3 | P003, P008, P009 | 3 |
| 4 | P002, P006, P011 | 3 |
| 5 | P001, P004 | 2 |

Contrôle de fin de partie (1.4) : 6 partitions, toutes `Leader: 1` ; somme des offsets de fin **0 → 360** (`0:135`, `1:75`, `2:30`, `3:45`, `4:45`, `5:30`).

### Mesure 4 — Messages lus (20 s)

| Groupe | `auto.offset.reset` | 1er lancement (20 s) | 2e lancement (20 s) |
|---|---|---|---|
| `explo-earliest` | earliest | **5 533** | **252** |
| `explo-latest` | latest | **241** | **252** |

### Mesure 5 — `--describe` sur `explo-earliest` après un arrêt propre (partition 0)

| Colonne | Valeur relevée |
|---|---|
| CURRENT-OFFSET | **1491** |
| LOG-END-OFFSET | **1494** |
| LAG | **3** |
| CONSUMER-ID affiché | **`-`** (« Consumer group 'explo-earliest' has no active members ») |

Sur les six partitions, le lag total est de 11 (offsets courants 527, 30, 1491, 934, 1402, 1401).

### Mesure 6 — Arrêt brutal puis relance

| Groupe | Mode | Messages lus avant le kill (dernier `lus=` affiché) | CURRENT-OFFSET total après le kill | Messages relus au redémarrage |
|---|---|---|---|---|
| `explo-auto` | automatique | **6 550** | **6 516** | **34** |
| `explo-manuel` | manuel (tous les 100) | **7 700** | **7 700** | **0** |

Compléments :
- Après relance à l'identique, la première ligne `ATTRIBUTION` n'apparaît qu'après **41,2 s** (`explo-auto`) et **43,5 s** (`explo-manuel`) : le broker attend l'expiration de la session du membre tué (`--describe` listait encore un membre pour `explo-manuel` juste après le kill).
- Les instances relancées ont lu 949 (`explo-auto`) et 917 (`explo-manuel`) messages en ~35 s : les relus plus les messages produits pendant l'absence (~12 msg/s pendant ~45 s).
- « Relus » = dernier `lus=` affiché − CURRENT-OFFSET total après le kill. Le lecteur n'affiche une ligne que tous les 50 messages, donc le total réel au moment du kill peut être un peu plus haut : 34 est un minimum. En mode manuel, le maximum théorique est 99 (commit tous les 100) ; ici le kill est tombé juste après le commit du 7 700ᵉ message, d'où 0.

### Mesure 7 — Attribution des partitions (`vitals-explo-groupe`, stratégie `range`)

| Situation | Partitions de cA | Partitions de cB | Partitions de cC |
|---|---|---|---|
| cA seul | 0, 1, 2, 3, 4, 5 | — | — |
| cA + cB | 0, 1, 2 | 3, 4, 5 | — |
| cA + cB + cC | 0, 1 | 2, 3 | 4, 5 |

À chaque arrivée, cA affiche un `RETRAIT` de toutes ses partitions suivi d'une nouvelle `ATTRIBUTION` : c'est le rééquilibrage eager.

### Mesure 8 — Débit par instance (60 s à trois)

| Instance | Messages lus pendant les 60 s à trois (différence des `lus=`) | Patients couverts (déduits de la mesure 3) |
|---|---|---|
| cA (partitions 0, 1) | **241** (577 − 336) | P005, P010, P012, P007 (4) |
| cB (partitions 2, 3) | **181** (349 − 168) | P003, P008, P009 (3) |
| cC (partitions 4, 5) | **301** (301 − 0) | P002, P006, P011, P001, P004 (5) |

Attendu : ~60 × nombre de patients (un relevé par seconde et par patient) = 240, 180 et 300.

### Mesure 9 — Un consommateur de trop

| Grandeur | Valeur relevée |
|---|---|
| Nombre de partitions du topic (`NB_PARTITIONS`) | **6** |
| Nombre d'instances lancées | **7** |
| Nombre d'instances affichant `partitions=[aucune]` | **1** |
| `lus=` de ces instances après 30 s | **0** |

### Mesure 10 — Délai avant réattribution à cA

| Scénario | Délai avant réattribution à cA | `session.timeout.ms` par défaut |
|---|---|---|
| A — arrêt propre (`close()` appelé) | **0,9 s** | 45 000 ms |
| B — arrêt brutal (processus tué, plus aucun heartbeat) | **47,5 s** | idem |

Sur Windows, la suspension (`Ctrl-Z`) et le `kill -9` ne font qu'une seule opération : le délai B est compté à partir de la mort du processus (dernier heartbeat à ≤ 3 s avant).

### Mesure 11 — Stratégies d'attribution (départ de cB)

| Stratégie | Lignes `RETRAIT` affichées par cA lors du départ de cB | Partitions listées dans ce `RETRAIT` |
|---|---|---|
| `range` | **1** | **0, 1, 2** (toutes ses partitions, puis `ATTRIBUTION` de 0 à 5) |
| `cooperative-sticky` | **0** | **aucune** (`ATTRIBUTION` incrémentale des seules partitions 5, 3, 1 ; cA garde 0, 2, 4 sans interruption) |

Délai de réattribution en coopératif : 1,3 s (groupe `vitals-explo-coop`). **Stratégie retenue dans le code de rendu : `cooperative-sticky` dans `consumer/consumer.py`** (le lecteur exploratoire reste en `range` par défaut).

### Mesure 12 — PostgreSQL

| Grandeur | Valeur relevée |
|---|---|
| Délai entre `up -d postgres` et `healthy` | **8,4 s** |
| Nombre de tables listées par `\dt` | **1** (`vitals_raw`) |
| Contrainte de clé primaire affichée par `\d vitals_raw` | **`pk_vitals_raw`** |

Le schéma a été créé automatiquement à partir de `/var/lib/postgres/initdb/01-init.sql`.

### Mesure 13 — Consommateur lancé depuis l'hôte (60 s)

| Grandeur | Valeur relevée |
|---|---|
| `lus=` affiché par le consommateur à l'arrêt | **23 641** |
| `inseres=` affiché à l'arrêt | **23 641** (`ignores_doublons=0`, `rejets=0`, débit 393,1 msg/s) |
| `lignes_totales` de `v_vitals_controle` | **23 641** |
| `messages_distincts` de `v_vitals_controle` | **23 641** |
| `patients_distincts` | **12** |

Le groupe, neuf et en `earliest`, a d'abord rattrapé l'historique accumulé depuis le début de la séance (d'où un débit très supérieur aux 12 msg/s du producteur).

### Mesure 14 — Chaîne complète en marche

| Grandeur | Valeur relevée |
|---|---|
| Nombre de services affichés par `docker compose ps` | **4** (`kafka`, `postgres`, `producer`, `consumer`) |
| Services en état `healthy` | **3** : `kafka`, `postgres`, `producer` (`consumer` n'a pas de healthcheck : `Up`) |
| Taille moyenne des lots (`LOT taille=`) | **72** (10 lots observés, tous de 72) |
| Croissance de `lignes_totales` sur 60 s | **720 lignes** (24 433 → 25 153) |

### Mesure 15 — Lag en régime nominal

| Instant | Somme des LAG |
|---|---|
| t0 | **11** |
| t0 + 30 s | **11** |
| t0 + 60 s | **11** |

### Mesure 16 — Lag en surcharge (`INTERVAL_S=0.005`, `BATCH_SIZE=1`, `--cpus 0.05`)

`docker compose exec producer env` confirme `INTERVAL_S=0.005`. Débit producteur observé : 2 245 msg/s ; débit consommé observé : 30,7 msg/s.

| Instant | Somme des LAG | Écart avec le relevé précédent |
|---|---|---|
| t0 | **15 491** | — |
| t0 + 30 s | **82 212** | +66 721 |
| t0 + 60 s | **148 490** | +66 278 |
| t0 + 90 s | **214 839** | +66 349 |
| t0 + 120 s | **281 268** | +66 429 |

### Mesure 17 — Passage à deux consommateurs

| Grandeur | Valeur relevée |
|---|---|
| Somme des LAG juste avant le `--scale` | **281 268** |
| Somme des LAG 60 s après | **432 477** |
| Nombre de membres du groupe affichés par `--describe` | **2** (3 partitions chacun) |
| Le lag décroît-il, se stabilise-t-il ou continue-t-il de croître ? | **continue de croître** |

Retour à l'état nominal vérifié : `INTERVAL_S=1.0` dans le producteur et `NanoCpus=0` (plus de bridage) sur le consommateur recréé ; le retard accumulé (428 031) a été résorbé en 83 s.

### Mesure 18 — Comportement pendant et après la coupure de PostgreSQL (90 s)

| Observation | Valeur relevée |
|---|---|
| Le conteneur consommateur s'arrête-t-il ou redémarre-t-il ? | **ni l'un ni l'autre** : `running`, 0 redémarrage |
| Première ligne d'erreur affichée | `ERREUR PostgreSQL (OperationalError): server closed the connection unexpectedly -- lot de 72 conserve, offsets NON acquittes, reconnexion` |
| Somme des LAG après 90 s de coupure | **1 199** |
| `lignes_totales` avant la coupure | **491 761** |
| `lignes_totales` 60 s après le redémarrage de PostgreSQL | **493 753** |
| Somme des LAG 60 s après le redémarrage | **35** |
| Le consommateur a-t-il rattrapé son retard sans intervention ? | **oui** (lag 1 199 → 35) |

Pendant la coupure, le journal enchaîne `PostgreSQL indisponible (essai n/30)` ; le CURRENT-OFFSET total est resté à 491 833 de +10 s à +90 s.

### Mesure 19 — Preuve chiffrée de la reprise (SIGKILL en plein lot)

| Grandeur | Valeur relevée |
|---|---|
| `lignes_totales` avant le SIGKILL | **493 753** |
| `inseres=` de l'instance relancée après 60 s (ligne `Arret.`) | **816** |
| `ignores_doublons=` de l'instance relancée | **0** (`lus=816`, `rejets=0`, débit 13,5 msg/s) |
| Nombre de lignes renvoyées par la requête de doublons | **0** |
| Partitions où lignes ≠ étendue (avec un trou) | **aucune** (6 partitions : 123 705/123 705, 41 265/41 265, 30/30, 123 616/123 616, 123 615/123 615, 82 410/82 410) |

**Essai complémentaire (hors énoncé)** — pour voir l'idempotence agir, j'ai arrêté le consommateur et reculé les offsets du groupe de 1 000 par partition (`--reset-offsets --shift-by -1000`), puis relancé : `lus=5642`, `inseres=612`, **`ignores_doublons=5030`**, `rejets=0` ; `lignes_totales` 494 641 → 495 313 ; 0 ligne dupliquée, aucun trou.

## Questions

### Question 1.1
**Non** : 8 patients sur 12 ont changé de partition (P001 : 2 → 5, P002 : 1 → 4, P003 : 0 → 3, P004 : 2 → 5, P006 : 1 → 4, P008 : 0 → 3, P009 : 0 → 3, P011 : 1 → 4) ; seuls P005, P007, P010 et P012 sont restés en place. La partition d'une clé est `murmur2(clé) & 0x7fffffff` modulo le nombre de partitions : le hachage ne change pas, mais le modulo passe de 3 à 6, donc `h mod 6` vaut `h mod 3` ou `h mod 3 + 3`, et les clés qui tombent dans la seconde moitié (+3) changent de partition.

### Question 1.2
**Non** : la mesure 3 donne 3, 1, 0, 3, 3, 2 patients (moyenne 2) et la **partition 2 ne reçoit aucun patient**. Le hachage répartit les clés comme un tirage pseudo-aléatoire : avec seulement 12 clés pour 6 partitions, obtenir exactement 2 par partition serait très improbable (la loi des grands nombres n'a rien à lisser sur si peu de clés) ; il y aura toujours des partitions chargées et des partitions creuses ou vides. En partie 3, cela se traduit par des instances inégalement chargées (4, 3 et 5 patients pour cA, cB, cC) et par un plafond de parallélisme effectif de 5 consommateurs utiles, la partition 2 ne produisant rien.

### Question 1.3
La garantie cassée est celle de l'**ordre par clé** : en séance 1, tous les messages d'un patient allaient dans une seule partition et étaient donc lus dans l'ordre où ils avaient été écrits. Elle est cassée pour la fenêtre qui court de l'instant de l'`ALTER` jusqu'à l'expiration (rétention) des messages écrits avant lui : tant que les anciens messages de P001 (partition 2) coexistent avec les nouveaux (partition 5), un consommateur peut lire les nouveaux avant les anciens.

### Question 2.1
Le deuxième lancement a repris où le premier s'était arrêté grâce à l'**offset commité** du groupe, conservé par le broker dans `__consumer_offsets` : à l'arrêt propre, le groupe avait acquitté sa position (CURRENT-OFFSET 1491 sur la partition 0). `auto.offset.reset=earliest` ne s'applique que lorsqu'il n'existe aucun offset commité pour le couple (groupe, partition), ce qui n'était vrai qu'au tout premier lancement : d'où 5 533 messages lus la première fois contre 252 la seconde (≈ les 20 s de production à 12 msg/s).

### Question 2.2
Première façon : lancer la nouvelle version avec un **nouveau `group.id`** (par exemple `vitals-persister-v2`) et `auto.offset.reset=earliest`, qui relit tout l'historique disponible sans toucher au groupe existant. Seconde façon : **réinitialiser les offsets** du groupe existant (`kafka-consumer-groups --reset-offsets --to-earliest --execute`, consommateurs arrêtés), ce qui rejoue tout mais perturbe le groupe en place et oblige à l'arrêter. En production, je choisirais le nouveau groupe : les deux versions tournent en parallèle et indépendamment, et on peut basculer ou revenir en arrière sans rien détruire.

### Question 2.3
Le risque de **perte** est dans le mode **automatique** : le client acquitte périodiquement ce qui a été remis à l'application, que le traitement soit terminé ou non, donc un arrêt après l'acquittement mais avant la fin du traitement perd le message (offset commité, traitement jamais effectué). Le risque de **traitement en double** est dans le mode **manuel** avec `commit` après le traitement : un arrêt entre la fin du traitement et le `commit` fait relire jusqu'à 99 messages déjà traités (ici 0 relu car le kill est tombé juste après un commit, mais le maximum théorique est 99). Dans les deux cas tout dépend de l'ordre « acquittement / traitement » : acquitter avant ou indépendamment du traitement donne du *at-most-once* (perte possible), acquitter après donne du *at-least-once* (doublon possible). C'est pourquoi la partie 4 écrit le lot d'abord, acquitte ensuite, et rend les doublons inoffensifs par la clé primaire.

### Question 3.1
**Non** : cA, cB et cC ont lu 241, 181 et 301 messages, soit environ 60 × leur nombre de patients (4, 3 et 5). L'écart vient directement de la répartition inégale de la mesure 3 : le nombre de messages lus par une instance suit le nombre de patients portés par ses partitions, pas le nombre de partitions (cB en a deux, mais la partition 2 est vide).

### Question 3.2
La borne supérieure du parallélisme de consommation d'un topic est son **nombre de partitions** (6 ici) : au-delà, les instances en trop restent sans partition (une des 7 instances affiche `partitions=[aucune]` et lit 0 message) ; en pratique la borne utile est même de 5 puisque la partition 2 est vide. Pour la dépasser, il faudrait **augmenter le nombre de partitions** du topic (ou répartir autrement les clés). Cette modification n'agirait que sur les messages à venir : les données déjà écrites resteraient dans leur ancienne partition, et les clés seraient redistribuées comme en question 1.3, avec la rupture de l'ordre par clé pendant la fenêtre de transition.

### Question 3.3
À l'arrêt propre, `close()` envoie une demande de départ explicite (LeaveGroup) au coordinateur, qui réattribue les partitions tout de suite : 0,9 s. Après un arrêt brutal, le coordinateur ne sait rien : il constate seulement l'absence de heartbeats (un toutes les 3 s) et attend l'expiration de `session.timeout.ms` (45 s) avant de déclarer le membre mort, d'où 47,5 s (45 s + environ 2,5 s de rééquilibrage). On ne peut pas descendre à 1 s car le délai doit rester plusieurs fois supérieur à l'intervalle des heartbeats : une pause de ramasse-miettes, une saturation du processeur ou une gigue réseau d'une seconde ferait déclarer morts des membres vivants, et le groupe se rééquilibrerait en boucle (le broker impose d'ailleurs un minimum, 6 s par défaut).

### Question 3.4
Avec la stratégie coopérative, seules les partitions qui changent de propriétaire sont retirées (cA n'a affiché aucun `RETRAIT` et n'a reçu que les partitions 5, 3 et 1, en gardant 0, 2 et 4) : un consommateur qui écrit en base n'a pas à interrompre ni à vider son lot pour les partitions qu'il garde, alors qu'en eager il doit tout rendre, écrire et acquitter son lot avant le rééquilibrage, et plus personne ne consomme pendant ce temps. **Stratégie utilisée dans le code de rendu : `cooperative-sticky`** (`consumer/consumer.py`), avec en plus un callback de retrait qui écrit et acquitte le lot en cours avant de rendre les partitions ; `lecteur.py` reste en `range` par défaut.

### Question 4.1
La clé primaire porte sur les coordonnées Kafka parce que `(partition, offset)` identifie **un message de façon unique et stable** : rejouer ce message redonne exactement la même clé, donc le rejeu est absorbé, et deux messages différents n'entrent jamais en collision. `(patient_id, event_time, vital_type)` n'est pas unique par construction : si un même patient émet deux relevés légitimes du même type à la même milliseconde (deux capteurs sur le lit, une correction de mesure, un producteur relancé qui réutilise un horodatage), le second serait rejeté par la clé primaire et une donnée légitime serait perdue. Ce second choix mélange en plus l'identité du message (déjà fournie par Kafka) avec son contenu métier.

### Question 4.2
**Oui**, `lignes_totales` et `messages_distincts` sont égaux (23 641 et 23 641 ; puis 496 129 et 496 129 au nettoyage). S'ils avaient différé (plus de lignes que de messages distincts), un même message Kafka aurait été inséré plusieurs fois, ce qui aurait révélé que la clé primaire était absente ou contournée et que l'idempotence ne tenait pas.

### Question 4.3
Le producteur débite 12 msg/s (12 patients × 1 relevé par seconde, mesuré 12,0 msg/s en séance 1 et croissance de 720 lignes en 60 s ici) ; remplir un lot de `BATCH_SIZE=200` prendrait donc 200 / 12 ≈ **16,7 s**, plus long que `BATCH_TIMEOUT_S=5 s`. C'est donc **le délai qui se déclenche** et jamais la taille : les lots observés font 72 messages, soit 6 salves de 12 relevés, ce qui s'explique vraisemblablement par le chronomètre remis à zéro après l'écriture (~60 ms) : la 5ᵉ salve arrive juste sous 5,0 s après le dernier envoi, et le lot ne part qu'à la 6ᵉ. La taille ne se déclenche que lorsque le consommateur rattrape un retard (lots de 200 observés à la reprise après la coupure de PostgreSQL).

### Question 5.1
Le lag est **stable** (11, 11 et 11 à 30 s d'intervalle). À l'équilibre il oscille entre 0 et environ 72 messages (12 msg/s × le délai d'environ 6 s entre deux acquittements, la taille de lot étant plafonnée par le délai et non par `BATCH_SIZE`), soit environ 36 en moyenne ; la valeur identique des trois relevés vient de ce que 30 s est un multiple de la période des lots (6 s), donc chaque relevé tombe à la même phase du cycle.

### Question 5.2
Dans l'ordre, il faudrait examiner : (1) le coût par message du consommateur lui-même (ici un `INSERT` et un acquittement par message avec `BATCH_SIZE=1`, plafonné à 5 % d'un cœur : 30,7 msg/s mesurés contre 2 245 msg/s produits) ; (2) les ressources de chaque instance (processeur, réseau) puis le nombre d'instances, borné par le nombre de partitions (6, dont une vide) ; (3) la capacité du stockage en aval, PostgreSQL. C'est cette dernière qui ne se corrige pas en ajoutant des consommateurs, car ils écrivent tous dans la même base (comme la limite du nombre de partitions, qui plafonne le nombre d'instances utiles) ; ici, le passage à deux instances n'a pas inversé la tendance (lag 281 268 → 432 477).

### Question 5.3
**Non**, les offsets n'ont pas avancé pendant la coupure : le CURRENT-OFFSET total est resté à 491 833 de +10 s à +90 s, alors que le lag montait de 11 à 1 199 (les messages continuaient d'arriver dans Kafka). C'est la conséquence directe de l'ordre `inserer_lot()` puis `commit()` : l'insertion échoue (`OperationalError`), l'exception est interceptée avant le `commit`, le lot de 72 est conservé en mémoire et la connexion est refaite, donc rien n'est acquitté tant que la base n'a pas accepté les lignes. Si j'avais inversé les deux instructions, les offsets auraient avancé sur des lots jamais écrits (environ 1 200 messages) et ces relevés auraient été définitivement perdus, car plus rien ne les aurait fait relire.

### Question 5.4
Dans ma mesure, `ignores_doublons` **est nul** (816 insérés, 0 ignoré, 0 doublon en base, aucun trou) : le SIGKILL est tombé entre deux lots, hors de la courte fenêtre (quelques dizaines de millisecondes toutes les ~6 s) qui sépare l'écriture du lot de son acquittement. Si le kill tombe dans cette fenêtre, le lot est déjà en base mais ses offsets ne sont pas acquittés : l'instance relancée relit ces messages, la clé primaire les refuse (`DO NOTHING`) et `ignores_doublons` devient non nul, ce qui prouve que le mécanisme fonctionne et non qu'il est défaillant. Je l'ai vérifié en reculant volontairement les offsets de 1 000 par partition : 5 030 messages rejoués ont tous été interceptés, sans une seule ligne en double.

### Question 5.5
Sur le **flux**, la chaîne garantit du **at-least-once** : le `commit` suit l'écriture, donc un incident peut faire relire des messages déjà traités, mais n'en perd aucun. Dans la **table**, l'état final est **exactly-once** : la clé primaire `(kafka_partition, kafka_offset)` et `ON CONFLICT DO NOTHING` absorbent les rejeux, donc chaque message n'y figure qu'une fois (0 doublon, 0 trou après SIGKILL et après rejeu volontaire). Ce n'est pas du *at-most-once*, qui supposerait d'acquitter avant d'écrire et de risquer la perte.

### Question 6.1
`docker compose down -v` détruit le volume `kafka-data`, donc le topic interne `__consumer_offsets` et les offsets commités de `vitals-persister` (496 129 au moment de la sauvegarde), ainsi que le volume `pg-data`, donc toutes les lignes de `vitals_raw` (496 129 également). Au prochain `up`, le broker repart vide, sans le topic `mimic-vitals` (création automatique désactivée : il faut le recréer) ni aucun groupe (vérifié : `--list` ne renvoie rien). Le consommateur, groupe neuf sans offset commité, repartirait alors du début avec `AUTO_OFFSET_RESET=earliest`, c'est-à-dire de l'offset 0 des nouveaux messages.

## Compléments

- Schéma sauvegardé avant le nettoyage avec `pg_dump --schema-only` (`sql/schema_dump.sql`, non inclus dans l'archive) : table `vitals_raw`, `ck_vital_type`, `pk_vitals_raw`, `idx_vitals_patient_time` et la vue `v_vitals_controle` sont bien présents.
- Nettoyage : `docker compose down -v`, `docker compose ps -a` vide, plus aucun volume `vitals`/`kafka-data`/`pg-data` ; après un `up -d kafka`, `--list` ne renvoie aucun groupe. `docker system df` : 10 images (3,118 GB, 94 % récupérables), 0 conteneur, 3 volumes anonymes sans rapport avec le projet, cache de build 880,7 MB.
