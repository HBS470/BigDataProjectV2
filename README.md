Plan de Projet Big Data : Prédiction de la Qualité de l'Air selon la Météo

  1. Objectif & Thème
  Thème : Environnement & Santé Publique.
  Objectif : Analyser et croiser les données de qualité de l'air avec les conditions météorologiques dans les grandes
  métropoles mondiales. Nous utiliserons le Machine Learning pour prédire les niveaux de pollution (comme les particules
  fines PM2.5 ou PM10) en fonction des facteurs météorologiques (vitesse du vent, précipitations, température).
  Pourquoi ce thème ? C'est un sujet très actuel, utile, qui justifie parfaitement le croisement de deux sources de
  données. Il permet de construire un modèle prédictif (Résultat très innovant) et de maximiser vos points.

  2. Sources de Données (100% Gratuites, Aucune Authentification Requise)
   1. API OpenAQ (Qualité de l'air) : (https://api.openaq.org/v2/measurements) Fournit des mesures historiques et en
      temps réel de la qualité de l'air dans les villes du monde entier.
   2. API Open-Meteo (Météo) : (https://api.open-meteo.com/v1/forecast) Fournit des données météorologiques actuelles et
      prévisionnelles.

  3. Architecture & Stack Technique (Maximisation des Points)
   - Orchestration : Apache Airflow (Docker) - Lancer tout en une fois (1 pt)
   - Stockage : LocalStack (émulation S3) - Ingestion dans un système de fichiers distribué (1 pt)
   - Traitement (Formatage & Combinaison) : Apache Spark (PySpark) - Formater en Parquet (2 pts), Utiliser Spark pour la
     combinaison (0.5 pt)
   - Machine Learning : Spark MLlib (Régression Linéaire ou Forêt Aléatoire) - Utiliser le ML pour produire le résultat
     (1 pt)
   - Indexation & Visualisation : Elasticsearch & Kibana - Indexer les données (2 pts) + Tableau de bord (2 pts)
   - Langage de Programmation : Python 3.x

  4. Convention de Nommage Propre (Bonus : 1 pt)
  Respect strict de la convention de nommage du Data Lake : data/<layer>/<group>/<dataEntity>/<dateVersion>/
   - Couche Brute (Raw) :
     - s3://datalake/data/raw/environment/openaq/YYYY-MM-DD/
     - s3://datalake/data/raw/weather/openmeteo/YYYY-MM-DD/
   - Couche Formatée (Parquet + Normalisée) :
     - s3://datalake/data/formatted/environment/openaq/YYYY-MM-DD/
     - s3://datalake/data/formatted/weather/openmeteo/YYYY-MM-DD/
   - Couche Combinée :
     - s3://datalake/data/combined/analytics/air_quality_prediction/YYYY-MM-DD/

  5. Étapes d'Implémentation (DAG Airflow)

  Étape 1 : Ingestion (Extraction)
   - Tâches Python dans Airflow pour appeler l'API OpenAQ pour une liste de 10-15 grandes villes (Paris, Pékin, Los
     Angeles, etc.) et l'API Open-Meteo pour les mêmes lieux.
   - Sauvegarde du JSON brut dans la couche raw sur S3 (LocalStack).

  Étape 2 : Formatage (Transformation)
   - Job Spark pour lire le JSON brut.
   - Aplatissement du JSON, renommage des colonnes, et normalisation des dates (tout convertir en UTC) - (1 pt).
   - Écriture en format Parquet dans le bucket formatted - (2 pts).

  Étape 3 : Combinaison & Machine Learning
   - Job Spark pour joindre les deux datasets formatted sur les clés géographiques (ville/coordonnées) et temporelles
     (heure/date).
   - Application de Spark MLlib pour entraîner un modèle qui prédit le taux de PM2.5/PM10 en fonction du vent (qui
     disperse la pollution) et de la pluie (qui lessive l'air).
   - Sauvegarde du dataset combiné incluant les prédictions (Actual vs Predicted) dans le bucket combined.

  Étape 4 : Indexation (Chargement)
   - Tâche pour lire les fichiers Parquet combined et les envoyer dans un index Elasticsearch.

  Étape 5 : Visualisation des Données
   - Création d'un tableau de bord Kibana pour afficher la pollution mondiale, les corrélations météo-pollution, et la
     précision du modèle de prédiction ML.
