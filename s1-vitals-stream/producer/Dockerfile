# 1. Image de base légère, version figée (jamais latest)
FROM python:3.12-slim

# 2. Comportement de Python : pas de .pyc, logs non bufferisés (docker logs)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. Répertoire applicatif
WORKDIR /app

# 4. Dépendances seules : change rarement -> couche mise en cache
COPY requirements.txt ./

# 5. Installation sans cache pip dans la couche
RUN pip install --no-cache-dir -r requirements.txt

# 6. Code applicatif : change souvent, donc APRÈS l'installation
COPY producer.py ./

# 7. Compte non privilégié (UID 10001) + droits sur le répertoire de travail
#    (/app/logs créé ici pour que le volume nommé hérite des bons droits)
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/logs \
    && chown -R appuser:appuser /app

# 8. Bascule définitive, avant CMD
USER appuser

# 9. Commande par défaut
CMD ["python", "producer.py"]
