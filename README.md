# 🚀 Projet : Construire et Tester une Infrastructure de Données

## 📌 Contexte
GreenAndCoop, un fournisseur coopératif français d'électricité renouvelable, souhaite améliorer ses prévisions de demande d'électricité en intégrant des données météorologiques issues de nouvelles sources (stations météo amateurs et open-data). En tant que Data Engineer, ma mission est de construire un pipeline fiable permettant de collecter, transformer et stocker ces données dans MongoDB sur AWS.

## 🏗 Objectifs
- Récupérer les données météo depuis différentes sources (fichiers Excel et JSON)
- Stocker les données brutes dans un bucket S3
- Transformer les données pour les rendre compatibles avec MongoDB
- Tester l'intégrité des données (valeurs manquantes, doublons, formats, etc.)
- Automatiser ces étapes

## 🛠 Stack Technique
- **Langage** : Python 3.13
- **Gestion d’environnement** : Poetry
- **ETL** : Airbyte
- **Stockage** : AWS S3
- **Base de données** : MongoDB

✨ Auteur : Maëva Beauvillain
📅 Date de début : Février 2025