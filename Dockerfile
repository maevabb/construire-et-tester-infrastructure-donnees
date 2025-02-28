
FROM python:3.13.0
WORKDIR /app

# Copie des fichiers pyproject.toml et poetry.lock
COPY pyproject.toml poetry.lock ./

# Installation de Poetry
RUN pip install poetry

# Installation des dépendances à l'aide de Poetry
RUN poetry install --no-root

# Copie des scripts
COPY transform_data.py .
COPY insert_data.py .

# Lancement du script
CMD ["poetry", "run", "python", "transform_data.py && insert_data.py"]