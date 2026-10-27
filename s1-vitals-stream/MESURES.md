# MESURES.md — Séance 1 : Conteneurs et flux

_Étudiant : PELLICANO Théo — Master 2 YNOV Campus Montpellier — Séance 1_

## Versions relevées (prérequis)

| Outil | Version relevée |
|---|---|
| Docker Engine | 29.8.2 (Docker Desktop) |
| Docker Compose | v5.5.1 |
| Python | 3.14.0 (`python` du poste, hors plage 3.11/3.12) ; 3.12.7 utilisé pour le `.venv` (lanceur `py -3.12`) et 3.12 dans le conteneur |

## Décisions retenues

| Décision | Valeur retenue | Justification |
|---|---|---|
| Image du producteur | `vitals-producer:0.1` | tag explicite, jamais `latest` |
| Nom du volume nommé des journaux | `vitals-logs` (nom réel Compose : `s1-vitals-stream_vitals-logs`) | nom explicite ; Compose le préfixe par le nom du projet (= nom du dossier) |
| Nom du réseau applicatif | `vitals-net` (nom réel : `s1-vitals-stream_vitals-net`) | réseau bridge dédié, partagé par tous les services jusqu'à la séance 6 ; DNS interne par nom de service |
| Valeur de `VITALS_TOPIC` | `mimic-vitals` | imposé par le projet HealthStream (tableau 4.1) |
| `interval` du healthcheck producer | `10s` | le test est trivial mais un intervalle plus court chargerait le conteneur pour rien ; 10 s donne une détection d'une panne en ≤ 30 s avec `retries: 3` |
| `retries` du healthcheck producer | `3` | tolère un échec isolé (test lent, pic de charge) sans qu'un service réellement mort reste « healthy » plus de ~30 s |
| Port hôte publié en face de 29092 | `29092` (`"29092:29092"`) | même numéro côté hôte et conteneur, simple à retenir ; c'est le listener `PLAINTEXT_HOST` annoncé en `localhost:29092` pour les clients du poste |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | un seul nœud KRaft joue les deux rôles (pas de ZooKeeper) |
| Facteur de réplication des topics internes | `1` (offsets, transaction state log ; `MIN_ISR` = 1) | un seul broker : toute valeur supérieure est impossible à satisfaire |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `"false"` | une faute de frappe sur un nom de topic produit une erreur visible au lieu de créer un topic fantôme |
| Nom du volume nommé des journaux Kafka | `kafka-data` (nom réel : `s1-vitals-stream_kafka-data`) | monté sur `/var/lib/kafka/data` : les données du broker survivent à `docker compose down` (mais pas à `down -v`) |
| `acks` | `all` | le leader n'acquitte qu'après réplication sur tout l'ISR : aucun message acquitté n'est perdu en cas de panne du leader (indispensable pour des signes vitaux médicaux) ; condition requise par l'idempotence. Avec un seul broker le surcoût de latence est nul |
| `linger.ms` | `20` | les 12 relevés d'un tour sont produits en quelques microsecondes : 20 ms d'attente les regroupe en un seul lot (moins de requêtes, meilleur débit) pour une latence ajoutée négligeable face à un échantillonnage de 1 s |
| `partitioner` | `murmur2_random` | seul choix interopérable avec le client Java (hachage murmur2 de la clé, comme le partitionneur par défaut Java) : le futur consommateur Flink et ce producteur Python placent une clé donnée dans la même partition. Le défaut de librdkafka, `consistent_random` (CRC32), n'est pas compatible |
| `INTERVAL_S` | `1.0` (1 Hz) | un moniteur ICU échantillonne environ une fois par seconde ; avec 12 patients, débit théorique = 12 msg/s |
| Partitions / réplication de `mimic-vitals` (3.3) | `--partitions 3 --replication-factor 1` | 3 partitions pour répartir les 12 patients (parallélisme possible avec jusqu'à 3 consommateurs en séance 2, où le nombre de partitions sera augmenté) ; réplication 1 car il n'y a qu'un broker (2 est impossible, cf. question 3.1) |
| `bootstrap.servers` | lu dans `KAFKA_BOOTSTRAP` : `localhost:29092` par défaut (hôte), `kafka:9092` dans Compose | même fichier depuis l'hôte (listener `PLAINTEXT_HOST`) et depuis un conteneur (listener interne `PLAINTEXT`) |
| `client.id` | `vitals-simulators` | nom du client visible côté broker (l'énoncé affiche « vitals-simulators1 » ; le « 1 » final a été lu comme un numéro de note du PDF) |
| `retries` | `5` | réémissions sur erreur transitoire (valeur imposée) |
| `enable.idempotence` | `True` | un retry ne crée ni doublon ni inversion d'ordre (valeur imposée) |
| `NB_PATIENTS` | `12` | un moniteur par lit, 12 lits (valeur imposée) |

## Mesures

### Mesure 1 — Image et couches

| Grandeur | Valeur relevée |
|---|---|
| Taille de `python:3.12-slim` | 179 MB (taille disque ; 45,4 MB de contenu compressé) |
| Taille de votre image finale (`vitals-producer:0.1`) | 193 MB |
| Nombre de couches non vides dans `docker history` | 9 (4 héritées de l'image de base + 5 ajoutées par notre Dockerfile) |
| Couche la plus volumineuse (instruction + taille) | `debian.sh --arch 'amd64' out/ 'trixie'` (rootfs Debian de l'image de base) : 87,7 MB. Parmi nos couches : `RUN pip install --no-cache-dir -r requirements.txt` : 11 MB |

### Mesure 2 — Cache de build

| Scénario de rebuild | Durée / étapes CACHED |
|---|---|
| Aucune modification | 6,3 s / 5 étapes `CACHED` |
| Modification de `producer.py` | 12,2 s / 3 étapes `CACHED` |
| Modification de `requirements.txt` | 25,0 s / 1 étape `CACHED` |

_Note : avec `--progress=plain`, l'étape `FROM` n'est pas comptée dans les `CACHED` ; il reste 5 étapes du Dockerfile (WORKDIR, COPY requirements, pip install, COPY producer.py, useradd). Sans modification, les 5 sont réutilisées. Modifier `producer.py` en invalide 2 (COPY producer.py et useradd, soit 3 `CACHED` restants). Modifier `requirements.txt` en invalide 4 (il ne reste que WORKDIR en cache), d'où le `pip install` relancé et la durée de ~25 s._

### Mesure 3 — Délai avant healthy (producer)

Délai observé avant healthy = **5,8 s** (`StartedAt` 09:33:55.67 → fin du premier test réussi 09:34:01.49 dans `.State.Health.Log`). Le premier test s'est exécuté ~5,7 s après le démarrage du conteneur, avant l'`interval` de 10 s ; `docker compose ps` affichait déjà `Up 7 seconds (healthy)`.

### Mesure 4 — Délai avant healthy (kafka)

Temps écoulé entre `docker compose up -d kafka` et le passage à `healthy` : **19,7 s** (chronométré, image déjà téléchargée). Côté Docker, `StartedAt` → fin du premier test réussi : 15,1 s.

### Mesure 5 — Répartition des clés

Topic `mimic-vitals`, 3 partitions, 5 messages envoyés avec le `kafka-console-producer` (clé = `patient_id`).

| Clé | Partition(s) et offsets observés |
|---|---|
| P001 | partition 2 : offsets 0, 1, 2 (3 messages : HR, SPO2, ABP_SYS) |
| P002 | partition 1 : offset 0 |
| P003 | partition 0 : offset 0 |

Offsets de fin (`kafka-get-offsets --time -1`) : `mimic-vitals:0:1`, `mimic-vitals:1:1`, `mimic-vitals:2:3` (soit 5 messages au total).

_Note : `kafka.tools.GetOffsetShell` (commande de l'énoncé) n'existe plus dans cette version (`ClassNotFoundException`) ; la commande équivalente `kafka-get-offsets` de l'image Confluent 7.8.0 a été utilisée._

### Mesure 6 — depends_on court vs service_healthy

Protocole : `docker compose down` (volumes conservés) puis `docker compose up -d`, attente du healthcheck de Kafka puis 15 s, lecture de `docker compose logs producer`.

| Forme de depends_on | Le producteur affiche-t-il des erreurs de connexion au démarrage ? | Nombre de lignes d'erreur avant stabilisation |
|---|---|---|
| `depends_on: [kafka]` | **Oui** : `FAIL ... kafka:9092/bootstrap: Connect to ipv4#172.18.0.2:9092 failed: Connection refused` (2 lignes, une 3e identique supprimée par librdkafka), le producteur démarre alors que le broker n'écoute pas encore | **50 lignes** comptées par le filtre (`%n\|`, `FAIL`, `ERROR`, `Error`) : parmi les 20 premières lignes relevées, 2 sont des erreurs de connexion (`FAIL`, antérieures à l'arrivée du broker) ; toutes les autres sont des avertissements `%4` (`GETPID`, `METADATAUPDATE`) pendant ~1 s à l'arrivée du broker. _Seules les 20 premières lignes ont été conservées, le décompte des `FAIL` au-delà est déduit de la chronologie (le broker écoute à partir de la 3e ligne d'avertissement)._ |
| `condition: service_healthy` | **Non** : aucune erreur de connexion (`FAIL` / `Connection refused`), Compose attend `Container vitals-kafka Healthy` avant de démarrer le producteur | 0 ligne d'erreur de connexion ; 29 lignes d'avertissement (`%4`) sur ~1 s juste après le démarrage (voir remarque) : **29 lignes** au total |

_Remarque : même avec `service_healthy`, quelques avertissements `%4` apparaissent au tout début : `Failed to acquire idempotence PID ... Coordinator load in progress` et `Partition mimic-vitals[...]: new leader-1 not found in cache`. Le healthcheck valide que l'API du broker répond, mais le coordinateur (PID idempotent) et les métadonnées finissent de se charger quelques centaines de millisecondes plus tard ; le producteur retente tout seul et aucun message n'est perdu. `service_healthy` supprime donc les échecs de connexion, pas toute trace de démarrage._

### Mesure 7 — Débit de production

Méthode : service `producer` arrêté, relevé des offsets de fin (`kafka-get-offsets --time -1`, somme des 3 partitions), puis un producteur jetable (même image, même réseau, même configuration, `docker compose run --rm --no-deps -e DUREE_S=60 producer`) tourne **exactement 60 s**, puis nouveau relevé. (`DUREE_S` est la variable facultative ajoutée au producteur pour arrêter la production à l'instant voulu.)

| Grandeur | Valeur |
|---|---|
| Somme des offsets de fin avant (t0) | 6641 (partitions : 3319 / 2213 / 1109) |
| Somme des offsets de fin après 60 s (t1) | 7361 (partitions : 3679 / 2453 / 1229) |
| Débit calculé (t1 - t0) / 60 | 720 / 60 = **12,00 msg/s** |
| Débit annoncé par le producteur à l'arrêt | `Bilan : envoyés=720 livrés=720 non livrés=0 durée=60.1s débit=12.0 msg/s` → 12,0 msg/s |
| Valeur de `INTERVAL_S` et de `NB_PATIENTS` utilisées | `INTERVAL_S=1.0`, `NB_PATIENTS=12` |

Mesure avec `INTERVAL_S=0.1` (même méthode, 60 s) :

| Grandeur | Valeur |
|---|---|
| Somme des offsets de fin avant → après | 7361 → 14501 (soit 7140 messages) |
| Débit mesuré avec `INTERVAL_S=0.1` | 7140 / 60 = **119,00 msg/s** (bilan du producteur : `envoyés=7140 livrés=7140 non livrés=0 durée=60.0s débit=118.9 msg/s`) |
| Écart avec la valeur théorique (12 / 0,1 = 120 msg/s) | **−1 msg/s, soit −0,83 %** |

### Mesure 8 — Coupure du broker

Protocole : producteur du service Compose en marche depuis 15 s, `docker compose stop kafka`, observation pendant 30 s, puis `docker compose start kafka` ; arrêt propre du producteur (SIGTERM) à la fin pour lire son bilan.

| Observation | Valeur relevée |
|---|---|
| Le processus producteur s'arrête-t-il ? | **Non** : `Running=true ExitCode=0` après 30 s de coupure, et encore 30 s après le redémarrage du broker |
| Message d'erreur exact affiché (première ligne) | `%6\|1791455144.154\|FAIL\|vitals-simulators#producer-1\| [thrd:kafka:9092/bootstrap]: kafka:9092/1: Disconnected (after 15827ms in state UP)` ; première vraie erreur de connexion juste après : `kafka:9092/1: Connect to ipv4#172.18.0.2:9092 failed: Connection refused (after 1ms in state CONNECT)` ; plus tard : `Failed to resolve 'kafka:9092': Name or service not known` (le conteneur arrêté disparaît du DNS du réseau) |
| Délai entre stop et la première erreur | **0,8 s** (≈ 1,0 s pour la première erreur `Connection refused`) ; la commande `docker compose stop kafka` elle-même a duré 5,1 s |
| Après `start`, la production reprend-elle sans relancer le script ? | **Oui** : le conteneur n'a pas redémarré ; le broker est `healthy` ~13 s après `start`, puis la somme des offsets progresse (15653 → 15737 en 5 s, file locale vidée + production courante) |
| Des messages produits pendant la coupure ont-ils été perdus ? | **Non** : bilan du producteur `envoyés=1248 livrés=1248 non livrés=0 durée=104.1s`, et 1248 messages réellement écrits dans le topic (somme des offsets 14501 → 15749, écart 0), sans doublon |

## Questions

### Question 1.1
**Non**, le second conteneur ne voit pas le fichier : `cat: /tmp/marqueur: No such file or directory`, d'où l'affichage de `fichier absent`. Chaque conteneur dispose de sa propre couche supérieure inscriptible au-dessus des couches en lecture seule de l'image ; le fichier écrit par `c1` n'existe que dans la couche de `c1` et disparaît avec lui.

### Question 1.2
Seul le scénario « modification de `producer.py` » changerait : un `COPY . ./` placé avant le `RUN pip install` serait invalidé par la modification du code, ce qui invaliderait aussi le `pip install`, donc ce rebuild réinstallerait les dépendances (durée proche des ~25 s du scénario `requirements.txt`, avec seulement 1 étape `CACHED` au lieu de 3). Les deux autres scénarios resteraient inchangés (aucune modification : tout en cache ; `requirements.txt` modifié : déjà tout invalidé à partir du `COPY`).

### Question 2.1
1. **Indépendance vis-à-vis de l'hôte** : un volume nommé est géré par Docker (`docker volume ls/inspect/rm`), sans dépendre d'un chemin ni de l'arborescence du poste (ici un chemin Windows contenant des espaces, des accents et un `&`) ; la configuration reste reproductible d'un poste à l'autre. Un bind mount `./logs` dépend du dossier courant.
2. **Droits et initialisation** : à la première utilisation, le volume nommé hérite du contenu et des droits du répertoire `/app/logs` de l'image (créé et possédé par `appuser`, UID 10001), donc le processus non-root peut y écrire. Avec un bind mount, le dossier hôte appartient à l'utilisateur de l'hôte et provoque souvent des erreurs de permission pour l'UID 10001 (en plus d'E/S plus lentes sur Docker Desktop).

### Question 3.1
Il n'y a qu'**un seul broker** dans le cluster : les réplicas d'une partition doivent être placés sur des brokers distincts, donc le facteur de réplication ne peut pas dépasser le nombre de brokers. En essayant `--replication-factor 2`, Kafka refuse la création et renvoie `InvalidReplicationFactorException: Unable to replicate the partition 2 time(s): The target replication factor of 2 cannot be reached because only 1 broker(s) are registered.` ; le topic n'est pas créé.

### Question 3.2
Oui, les trois messages de P001 sont tous dans la partition 2 et leurs offsets se suivent (0, 1, 2) : la clé est hachée et `hash(clé) mod 3` envoie toujours la même clé vers la même partition. Comme l'ordre n'est garanti qu'à l'intérieur d'une partition, les relevés de P001 sont relus exactement dans l'ordre d'écriture (HR, SPO2, ABP_SYS). En revanche, il n'y a aucune garantie d'ordre entre patients : P002 (partition 1) et P003 (partition 0, pourtant envoyé en 4e) sont relus avant/après P001 sans lien avec leur ordre d'envoi.

### Question 5.1
Oui : avec `INTERVAL_S=1.0` le débit mesuré (12,00 msg/s) est identique au débit théorique `NB_PATIENTS / INTERVAL_S = 12 / 1,0 = 12 msg/s` (écart 0 %), et avec `INTERVAL_S=0.1` il vaut 119,00 msg/s pour 120 msg/s théoriques, soit −0,83 %, donc sous le seuil de 5 % : aucun écart à justifier. Le léger déficit à 0,1 s vient du fait que chaque tour dure `INTERVAL_S` plus le temps de produire les 12 relevés et d'appeler `poll(0)` (environ 0,8 ms par tour, soit ~595 tours en 60 s au lieu de 600), car le `sleep` est placé après la production ; le producteur et le broker ne sont pas le facteur limitant, c'est le rythme imposé par le `sleep` qui fixe le débit.

### Question 5.2
Pendant la coupure, `produce()` ne fait qu'ajouter les relevés à la file d'attente locale de librdkafka (appel asynchrone) et le thread interne tente de se reconnecter en arrière-plan en affichant les `FAIL` sur stderr, d'où un processus qui continue à tourner sans exception. Avec `acks=all`, un message n'est considéré livré que lorsque le leader et tout l'ISR l'ont acquitté : tant que le broker est absent, aucun accusé n'arrive et les plusieurs centaines de messages produits pendant la coupure restent en file, puis sont réémis à la reconnexion. `enable.idempotence=True` (PID + numéros de séquence) garantit que ces réémissions et les `retries=5` ne créent ni doublon ni inversion d'ordre, ce que confirme l'écart de 0 entre les 1248 messages envoyés et les 1248 écrits dans le topic. La durée de tenue sans broker est limitée par le délai d'expiration des messages en file (`message.timeout.ms`, 300 s par défaut, à partir duquel un message non livré est signalé en échec dans `accuse_reception`) et par la taille de la file locale (`queue.buffering.max.messages`, 100 000 par défaut, soit ~2 h 20 à 12 msg/s, au-delà duquel `produce()` lève `BufferError`) ; c'est l'expiration à 5 minutes qui arrive en premier.

### Question 6.1
`docker compose down` supprime les conteneurs et le réseau mais **conserve les volumes nommés** : le volume `kafka-data` (journaux du broker) survit, donc au prochain `up` le broker redémarre avec ses données, retrouve le topic `mimic-vitals` (même TopicId) et ses messages et offsets, et le producteur reprend là où il en était (vérifié en 4.5 : après `down` puis `up`, les logs référencent le même TopicId `bdpIYTg5S9yvLeevXAFIsQ` et les offsets continuent). `docker compose down -v` supprime en plus les volumes nommés déclarés dans le fichier (`kafka-data` et `vitals-logs`) : les données du broker sont détruites, et au prochain `up` le broker repart d'un état vierge sans topic (la création automatique est désactivée, il faut donc recréer `mimic-vitals` avant de produire), avec offsets repartant à 0.

_Vérification expérimentale :_ après un `docker compose down` simple, le volume `s1-vitals-stream_kafka-data` reste listé et, au `up -d kafka` suivant, `kafka-topics --list` renvoie `mimic-vitals` (et `__consumer_offsets`) avec les offsets de fin intacts (7873 + 5249 + 2627 = 15 749 messages, valeur exacte de la fin de la partie 5.2). Après `docker compose down -v`, les volumes `s1-vitals-stream_kafka-data` et `s1-vitals-stream_vitals-logs` sont supprimés et, au `up -d kafka` suivant, `kafka-topics --list` ne renvoie plus rien.

## Compléments


### Parties 1.3, 2.2 et 2.3 — relevés demandés par l'énoncé
- `docker run --rm vitals-producer:0.1 id` → `uid=10001(appuser) gid=10001(appuser) groups=10001(appuser)` ; `docker image inspect` → `User=appuser Cmd=[python producer.py]`.
- Noms réels après `docker compose up` : réseau `s1-vitals-stream_vitals-net`, volume `s1-vitals-stream_vitals-logs` (préfixe = nom du projet Compose, c'est-à-dire du dossier).
- `docker volume inspect s1-vitals-stream_vitals-logs --format '{{.Mountpoint}}'` → `/var/lib/docker/volumes/s1-vitals-stream_vitals-logs/_data` (chemin à l'intérieur de la VM de Docker Desktop, pas sur le disque Windows).
- Résolution DNS (conteneur `busybox` jetable sur `s1-vitals-stream_vitals-net`) : `nslookup producer` → serveur `127.0.0.11` (DNS interne de Docker), `producer` → `172.18.0.2`.

### Partie 4.4 — exécution depuis l'hôte
- `python` du poste est en 3.14.0 : `confluent-kafka==2.6.1` n'a pas de paquet précompilé pour cette version. Le `.venv` a donc été créé avec Python 3.12.7 (`py -3.12`), puis `pip install -r producer/requirements.txt` (confluent-kafka 2.6.1, wheel `cp312-win_amd64`).
- Exécution de 12 s sur `localhost:29092` : `Bilan : envoyés=144 livrés=144 non livrés=0 durée=12.0s débit=12.0 msg/s`.
- Relecture complète du topic : 149 messages (5 de la partie 3 + 144). Chaque patient reste dans **une seule partition** : P001 → 2, P002 → 1, P003 → 0 (identique aux envois du `kafka-console-producer` Java de la partie 3, ce qui confirme l'interopérabilité de `murmur2_random`), P004 → 2, P005 → 0, P006 → 1, P007 → 1, P008–P010 → 0, P011 → 1, P012 → 0.
- Le producteur accepte une variable facultative `DUREE_S` (0 par défaut = tourne jusqu'à SIGINT/SIGTERM) utilisée pour automatiser les mesures ; elle ne change pas le comportement par défaut.
- La commande de relecture de l'énoncé (`--max-messages 10` sans `--from-beginning`) ne lit que les messages arrivés après son lancement ; la relecture a donc été faite avec `--from-beginning`.

### Nettoyage final
`docker compose down -v` puis vérifications : `docker compose ps -a` vide, `docker volume ls | grep vitals` et `docker network ls | grep vitals` ne renvoient rien. `docker system df` : 6 images (1,927 GB, 90 % récupérables), 0 conteneur, 3 volumes anonymes sans rapport avec le projet, cache de build 208,1 MB. Les images n'ont volontairement pas été supprimées. Le `.venv` n'est pas inclus dans l'archive.
