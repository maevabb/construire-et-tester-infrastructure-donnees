# 🚀 Projet : Construire et Tester une Infrastructure de Données
✨ Auteur : Maëva Beauvillain
📅 Date de début : Février 2025
📅 Dernière MAJ : 11 mars 2025

## 📌 Contexte
GreenAndCoop, un fournisseur coopératif français d'électricité renouvelable, souhaite améliorer ses prévisions de demande d'électricité en intégrant des données météorologiques issues de nouvelles sources (stations météo amateurs et open-data). En tant que Data Engineer, ma mission est de construire un pipeline fiable permettant de collecter, transformer et stocker ces données dans MongoDB sur AWS.

## 🏗 Objectifs
- Collecter des données provenant de stations météorologiques en open-data (InfoClimat, Weather Underground).
- Intégrer ces données dans une base MongoDB, adaptée aux spécificités des différentes sources.
- Mettre en place un pipeline de collecte, traitement, et stockage des données.
- Assurer la qualité des données en analysant leur intégrité (taux d'erreurs, données manquantes, etc.).
- Déployer cette solution sur AWS, en vue d'une intégration avec SageMaker pour les travaux de Machine Learning.

## 🛠 Stack Technique
- **Langage** : Python 3.13
- **Gestion d’environnement** : Poetry
- **ETL** : Airbyte
- **Stockage** : AWS S3
- **Base de données** : MongoDB

## Schema de la BDD

```
{
    "id_station": "string",
    "station_info": {
        "name": "string",
        "latitude": "double",
        "longitude": "double",
        "elevation": "int",
        "city": "string",
        "state": "string",
        "type": "string",
        "hardware": "string",
        "software": "string",
        "license": {
            "license": "string",
            "url": "string",
            "source": "string",
            "metadonnees": "string"
        }
    },
    "datetime": "string",
    "weather_data": {
        "temperature": "double",
        "pressure": "double",
        "humidity": "int",
        "dew_point": "double",
        "visibility": "double",
        "wind_speed": "double",
        "wind_gust": "double",
        "wind_direction": "double",
        "precip_1h": "double",
        "precip_3h": "double",
        "precip_accum": "double",
        "precip_rate": "double",
        "solar": "double",
        "uv": "int",
        "snow_depth": "double",
        "nebulosity": "double",
        "weather_wmo": "double"
    }
}
```

## Dictionnaire des données
| Champs           | Indic                          | Unité de mesure |
|------------------|--------------------------------|------------------|
| id_station       | id of the station              |                  |
| datetime         | timestamp in format yyyy-mm-dd hh:mm:ss |              |
| temperature      | temperature                    | °C               |
| pressure         | mean sea level pressure        | hPa              |
| humidity         | relative humidity              | %                |
| dew_point        | dewpoint                       | °C               |
| visibility       | horizontal visibility          | m                |
| wind_speed       | mean wind speed                | km/h             |
| wind_gust        | wind gust                      | km/h             |
| wind_direction   | wind direction                 | degrees          |
| precip_1h        | precipitation over 1h          | mm               |
| precip_3h        | precipitation over 3h          | mm               |
| precip_accum     | accumulated precipitation      | mm               |
| precip_rate      | precipitation rate             | mm               |
| solar            | solar radiation                | W/m²             |
| uv               | UV index                       | UV               |
| snow_depth       | snow depth                     | cm               |
| nebulosity       | cloud cover                    | octats           |
| weather_wmo      | present weather                |                  |


## Logique de transformation des données 
Ce script transforme des données météorologiques extraites depuis des fichiers JSON situés dans un bucket S3, avec les étapes suivantes :

### 1. Extraction des données depuis S3
- Connexion au bucket S3 p8-airbyte-greenandcoop.
- Téléchargement du fichier JSON contenant des données météorologiques infoclimat.
- Parsing du fichier JSON pour en extraire les informations relatives aux stations météo sous la clé _airbyte_data.

### 2. Transformation des données des stations
- Pour chaque station, certains champs sont renommés et de nouveaux champs sont ajoutés (ex. city, state).
- Les champs des stations sont ensuite réorganisés pour uniformiser les données.

### 3. Ajout des stations weather_underground
- Deux stations supplémentaires provenant de Weather Underground sont ajoutées à la liste des stations existantes.

### 4. Sauvegarde sur S3
- Les données transformées sont converties en JSON et sauvegardées dans un nouveau fichier dans le bucket S3.

### 5. Transformation des données horaires (Weather Underground)
- Les jeux de données weather_underground (Belgique et France) sont extraits depuis S3, traités pour ajouter et ajuster les dates et heures.
- Les colonnes Temperature, Dew Point, Pressure, Gust et autres sont converties aux unités métriques (Celsius, km/h, hPa, mm).
- Les directions du vent sont converties en degrés.
- Les valeurs textuelles sont nettoyées de toute unité superflue (comme "°F", "%").
- Le jeux de données infoclimat est extrait depuis S3
- Les noms et ordre des champs sont harmonisés

### 6. Conversion et sauvegarde sur S3
- Les données transformées sont converties en JSON et sauvegardées dans un nouveau fichier dans le bucket S3.

## 🔧 Infrastructure et Déploiement
- **Docker** : L’ensemble des services, y compris MongoDB et les scripts de transformation, a été conteneurisé à l’aide de Docker Compose. Le fichier docker-compose.yml définit un replica set MongoDB ainsi qu’un service data_pipeline qui exécute les scripts Python.
- **Réseau** : Tous les conteneurs sont connectés via un réseau Docker bridge. Les scripts de transformation interagissent avec MongoDB via un alias réseau interne.

## 🔍 Tests et Qualité des Données
- **Vérification de l’intégrité** : Les données sont vérifiées à chaque étape du pipeline pour s’assurer que les champs requis sont présents, que les types de données sont corrects et que les valeurs manquantes sont gérées de manière appropriée.
- **Tests automatisés** : Des tests unitaires et d’intégration ont été mis en place pour valider le schéma de la base de données et les transformations de données. Ces tests s’assurent que les indices sont correctement appliqués et que les performances sont optimales.
- **Analyse des performances** : Des tests d’accessibilité ont été effectués pour mesurer les temps de réponse avec et sans index, afin de garantir un accès rapide aux données pour les Data Scientists.
