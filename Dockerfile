FROM python:3.13.0
WORKDIR /app

# Installer Poetry globalement
RUN pip install poetry

# Copier les fichiers de dépendances
COPY pyproject.toml poetry.lock ./

# Installer les dépendances du projet
RUN poetry install --no-root --no-interaction

# Copier le reste du code
COPY transform_data.py .
COPY insert_data.py .
COPY data_accessibility.py .
COPY tests/ tests/
