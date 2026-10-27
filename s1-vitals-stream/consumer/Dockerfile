# Même patron que le producteur de la séance 1 (python:3.12-slim) : seule la commande change.

# 1. Image de base légère, version figée (jamais latest) : la même que le producteur
FROM python:3.12-slim

# 2. Comportement de Python : pas de .pyc, logs non bufferisés (docker logs en temps réel)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Répertoire applicatif
WORKDIR /app

# 4. Dépendances seules : change rarement -> couche mise en cache
#    (confluent-kafka et psycopg2-binary sont des roues précompilées)
COPY requirements.txt ./

# 5. Installation sans cache pip dans la couche
RUN pip install --no-cache-dir -r requirements.txt

# 6. Code applicatif : change souvent, donc APRÈS l'installation
#    (seul consumer.py : lecteur.py est un outil d'exploration lancé depuis l'hôte)
COPY consumer.py ./

# 7. Compte non privilégié (UID 10001), le même que le producteur de la séance 1
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app

# 8. Bascule définitive, avant CMD
USER appuser

# 9. Commande par défaut : le consommateur de persistance (partie 4)
CMD ["python", "consumer.py"]
