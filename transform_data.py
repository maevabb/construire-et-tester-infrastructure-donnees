# %%
import boto3
import pandas as pd
from io import StringIO
import json


# %% [markdown]
# # STATIONS

# %%
# Connexion au bucket S3 
bucket_name = "p8-airbyte-greenandcoop"
s3_client = boto3.client("s3")

response = s3_client.get_object(Bucket=bucket_name, Key="GreenAndCoop InfoClimat/infoclimat/2025_02_13_1739448920127_0.jsonl")

json_content = response["Body"].read().decode("utf-8")

data = [json.loads(line) for line in json_content.splitlines()]

# Extraire les données imbriquées sous "_airbyte_data" et "stations"
stations = [station for entry in data for station in entry["_airbyte_data"]["stations"]]


# %%
for doc in stations:
    doc["id_station"] = doc.pop("id")  # Renommer "id" en "id_station"
    doc["city"] = doc["name"]  # Ajouter un champ "city"
    doc["state"] = "France"  # Ajouter un champ "state"

# %%
# Réorganisation des champs

for i, doc in enumerate(stations):
    stations[i] = {
        "id_station": doc["id_station"],
        "name": doc["name"],
        "latitude": doc["latitude"],
        "longitude": doc["longitude"],
        "elevation": doc["elevation"],
        "city": doc["city"],
        "state": doc["state"],
        "type": doc["type"],
        "license": doc["license"],
    }


# %%
#Ajout des documents avec les metadonnees de weather_underground

weather_stations = [
    {
        "id_station": "ILAMAD25",
        "name": "La Madeleine",
        "latitude": 50.659,
        "longitude": 2.877,
        "elevation": 23,
        "city": "La Madeleine",
        "state": "France",
        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6"    
    },
    {
        "id_station": "IICHTE19",
        "name": "WeerstationBS",
        "latitude": 51.092,
        "longitude": 2.999,
        "elevation": 15,
        "city": "Ichtegem",
        "state": "Belgium",
        "hardware": "other",
        "software": "EasyWeatherV1.6.6"    
    }    
]

stations.extend(weather_stations)

# %%
# Convertir station en format JSON
stations_json = json.dumps(stations, indent=4)

s3_file_key = "data_transformed/stations.json"

# Télécharger le fichier sur S3
s3_client.put_object(Body=stations_json, Bucket=bucket_name, Key=s3_file_key)

print(f"Le fichier JSON a été téléchargé sur le bucket S3 : {bucket_name}/{s3_file_key}")

# %% [markdown]
# # HOURLY DATA

# %% [markdown]
# ## Weather Underground


# %%
# Conversion json - df
def load_airbyte_data_from_s3(bucket_name: str, file_key: str) -> pd.DataFrame:
    """
    Télécharge un fichier JSON depuis S3 et extrait les données imbriquées sous "_airbyte_data"
    pour les charger dans un DataFrame.

    Parameters:
    - bucket_name (str): Le nom du bucket S3.
    - file_key (str): La clé du fichier dans le bucket S3.

    Returns:
    - pd.DataFrame: Le DataFrame contenant les données extraites sous "_airbyte_data".
    """
    s3_client = boto3.client("s3")
    
    # Télécharger le fichier depuis S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    
    # Lire le contenu et décoder en UTF-8
    json_content = response["Body"].read().decode("utf-8")
    
    # Charger le JSON dans une liste d'objets Python
    data = [json.loads(line) for line in json_content.splitlines()]
    
    # Extraire les données imbriquées sous "_airbyte_data"
    airbyte_data = [entry["_airbyte_data"] for entry in data]
    
    # Convertir cette liste en DataFrame
    df = pd.DataFrame(airbyte_data)
    
    return df


# %%
weather_be_file_key = "GreenAndCoop Weather Underground/weather_underground_be/2025_02_25_1740480795604_0.jsonl"
weather_fr_file_key = "GreenAndCoop Weather Underground/weather_underground_fr/2025_02_25_1740480767021_0.jsonl"

weather_be = load_airbyte_data_from_s3(bucket_name, weather_be_file_key)
weather_fr = load_airbyte_data_from_s3(bucket_name, weather_fr_file_key)


# %%
# Ajout date fichiers weather
def add_dates_df(df, start_date_str="2024-10-01"):
    """
    Ajoute une colonne 'datetime' en combinant les colonnes 'date' et 'Time', 
    tout en supprimant les lignes vides. La date s'incrémente à chaque ligne vide 
    et la colonne 'Time' est convertie en timedelta. La colonne 'datetime' 
    contient la date et l'heure sous forme de timestamp.

    Paramètres :
    - df (DataFrame) : DataFrame à traiter.
    - start_date_str (str) : Date de départ sous forme de chaîne de caractères (format 'YYYY-MM-DD').

    Retourne :
    - df_dated (DataFrame) : DataFrame avec la colonne 'datetime' et sans les lignes vides.
    """
    # Supprimer la première ligne du DataFrame
    df = df.drop(index=0).reset_index(drop=True)
    
    # Convertir la chaîne de caractères en un objet datetime
    start_date = pd.to_datetime(start_date_str)
    current_date = start_date
    date_column = []

    # Identifier les lignes complètement vides
    empty_rows = df.isnull().all(axis=1)

    # Parcourir chaque ligne pour attribuer la date
    for is_empty in empty_rows:
        if is_empty:
            current_date += pd.Timedelta(days=1)  # Changer de date après chaque ligne vide
        date_column.append(current_date.strftime("%Y-%m-%d"))

    # Ajouter la colonne 'date' au DataFrame
    df["date"] = date_column

    # Convertir la colonne 'date' en type datetime
    df["date"] = pd.to_datetime(df["date"])

    # Convertir 'Time' en timedelta
    df['Time'] = pd.to_timedelta(df['Time'])

    # Combiner 'date' et 'time' en une seule colonne 'datetime'
    df['datetime'] = df['date'] + df['Time']

    # Convertir datetime en str
    df['datetime'] = df['datetime'].astype(str)

    # Supprimer les colonnes 'Time' et 'date'
    df = df.drop(columns=['Time', 'date'])

    # Supprimer les lignes vides et réindexer
    df_dated = df[~empty_rows].reset_index(drop=True)

    return df_dated

# %%
weather_be = add_dates_df(weather_be, start_date_str="2024-10-01")
weather_fr = add_dates_df(weather_fr, start_date_str="2024-10-01")

# %%
# Ajout id station
weather_be["id_station"] = "IICHTE19"
weather_fr["id_station"] = "ILAMAD25"

# %%
# Conversion °F en °C
def convert_fahrenheit_to_celsius(df, columns):
    for col in columns:
        # Vérifier que la colonne contient des chaînes de caractères
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace("\xa0", "", regex=False) 
            df[col] = df[col].str.replace("°F", "", regex=False)
        
        # Convertir en float et appliquer la conversion
        df[col] = df[col].astype(float)     
        df[col] = ((df[col] - 32) * 5/9).round(2)  # Conversion °F en °C

    return df

dfs = [weather_fr, weather_be]
columns_to_convert = ["Temperature", "Dew Point"]

for df in dfs:
    df = convert_fahrenheit_to_celsius(df, columns_to_convert)

# %%
# Conversion inHg en hPa
def convert_inHg_to_hPa(df, columns):
    for col in columns:
        # Vérifier que la colonne contient des chaînes de caractères
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace("\xa0", "", regex=False)
            df[col] = df[col].str.replace("in", "", regex=False)
        
        # Convertir en float et appliquer la conversion
        df[col] = df[col].astype(float)
        df[col] = (df[col]* 33.8639).round(2)  # Conversion inHg en hPa

    return df

columns_to_convert = ["Pressure"]

for df in dfs:
    df = convert_inHg_to_hPa(df, columns_to_convert)

# %%
# Conversion mp/h en km/h
def convert_mph_to_kmh(df, columns):
    for col in columns:
        # Vérifier que la colonne contient des chaînes de caractères
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace("\xa0", "", regex=False)
            df[col] = df[col].str.replace("mph", "", regex=False)
        
        # Convertir en float et appliquer la conversion
        df[col] = df[col].astype(float)
        df[col] = (df[col]* 1.60934).round(2)  # Conversion mph en kmh

    return df

columns_to_convert = ["Gust", "Speed"]

for df in dfs:
    df = convert_mph_to_kmh(df, columns_to_convert)

# %%
# Conversion in en mm
def convert_in_to_mm(df, columns):
    for col in columns:
        # Vérifier que la colonne contient des chaînes de caractères
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace("\xa0", "", regex=False)
            df[col] = df[col].str.replace("in", "", regex=False)
        
        # Convertir en float et appliquer la conversion
        df[col] = df[col].astype(float)
        df[col] = (df[col]* 25.4).round(2)  # Conversion in en mm

    return df

columns_to_convert = ["Precip. Rate.", "Precip. Accum."]

for df in dfs:
    df = convert_in_to_mm(df, columns_to_convert)

# %%
# Conversion Wind direction
def convert_wind_direction_to_degrees(df, column_name):
    # Dictionnaire de correspondance
    direction_dict = {
        "North": 0,
        "NNE": 22.5,
        "NE": 45,
        "ENE": 67.5,
        "East": 90,
        "ESE": 112.5,
        "SE": 135,
        "SSE": 157.5,
        "South": 180,
        "SSW": 202.5,
        "SW": 225,
        "WSW": 247.5,
        "West": 270,
        "WNW": 292.5,
        "NW": 315,
        "NNW": 337.5
    }
    
    # Appliquer la conversion
    df[column_name] = df[column_name].map(direction_dict)

    # Convertir la colonne en float
    df[column_name] = df[column_name].astype(float)
    
    return df

for df in dfs:
    df = convert_wind_direction_to_degrees(df, "Wind")


# %%
# Suppression des caractères
def delete_unities(df, columns):
    for col in columns:
        # Vérifier que la colonne contient des chaînes de caractères
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace("\xa0", "", regex=False)  # Supprime les espaces
            df[col] = df[col].str.replace("w/m²", "", regex=False)  # Supprime "w/m²"
            df[col] = df[col].str.replace("%", "", regex=False)  # Supprime "%"
    
    return df

columns_to_convert = ["Solar", "Humidity"]

for df in dfs:
    df = delete_unities(df, columns_to_convert)

# %%
# Conversion des types
for df in dfs:
    df["Humidity"] = df["Humidity"].astype(int)
    df["UV"] = df["UV"].astype(int)
    df["Solar"] = df["Solar"].astype(float)

# %%
# Nommage des colonnes et tri
rename_dict = {
    "Temperature": "temperature",
    "Pressure": "pressure",
    "Gust": "wind_gust",
    "Dew Point": "dew_point",
    "Precip. Rate.": "precip_rate",
    "Solar": "solar",
    "Precip. Accum.": "precip_accum",
    "Humidity": "humidity",
    "UV": "uv",
    "Speed": "wind_speed",
    "Wind": "wind_direction"
}

# Ordre des colonnes souhaité
desired_column_order = [
    "id_station", "datetime","temperature","pressure","humidity","dew_point","wind_speed","wind_gust","wind_direction","precip_accum","precip_rate","solar",
    "uv"
]

weather_fr = weather_fr.rename(columns=rename_dict)
weather_fr = weather_fr[desired_column_order]

weather_be = weather_be.rename(columns=rename_dict)
weather_be = weather_be[desired_column_order]  

# %% [markdown]
# ## InfoClimat

# %%

# Télécharger le fichier depuis S3
response = s3_client.get_object(Bucket="p8-airbyte-greenandcoop", Key="GreenAndCoop InfoClimat/infoclimat/2025_02_13_1739448920127_0.jsonl")
json_content = response["Body"].read().decode("utf-8")


data = [json.loads(line) for line in json_content.splitlines()]

# Extraire les données imbriquées sous "_airbyte_data" pour chaque station
def extract_station_data(station_id, data):
    """
    Extrait les données horaires pour une station donnée.
    Si les données n'existent pas pour cette station, renvoie une liste vide.
    """
    return [
        entry["_airbyte_data"]["hourly"].get(station_id, [])
        for entry in data
    ]

# Liste des ID des stations
station_ids = ["07015", "00052", "000R5", "STATIC0010"]

# Extraire les données pour chaque station et les convertir en df
dfs = []
for station_id in station_ids:
    data_station = extract_station_data(station_id, data)
    station_df = pd.DataFrame([item for sublist in data_station for item in sublist])
    dfs.append(station_df)

# Concaténer les DataFrames
infoclimat = pd.concat(dfs, ignore_index=True, join='outer')


# %%
# Nommage et tri des colonnes
rename_dict = {
    "dh_utc": "datetime",
    "pression": "pressure",
    "vent_rafales": "wind_gust",
    "point_de_rosee": "dew_point",
    "humidite": "humidity",
    "vent_moyen": "wind_speed",
    "vent_direction": "wind_direction",
    "visibilite": "visibility",
    "pluie_3h": "precip_3h",
    "pluie_1h": "precip_1h",
    "neige_au_sol": "snow_depth",
    "nebulosite": "nebulosity",
    "temps_omm": "weather_wmo"
}

# Ordre des colonnes souhaité
desired_column_order = [
    "id_station", "datetime","temperature","pressure","humidity","dew_point","visibility","wind_speed","wind_gust","wind_direction","precip_1h","precip_3h",
    "snow_depth","nebulosity","weather_wmo"
]

infoclimat = infoclimat.rename(columns=rename_dict)
infoclimat = infoclimat[desired_column_order]

# %%
# Typage des champs
float_columns=["temperature","pressure","dew_point","wind_speed","wind_gust","wind_direction","visibility","precip_1h","precip_3h","snow_depth","nebulosity","weather_wmo"]

for col in float_columns:
    infoclimat[col] = pd.to_numeric(infoclimat[col], downcast="float")

infoclimat["humidity"] = infoclimat["humidity"].astype(int)

infoclimat["pressure"] = infoclimat["pressure"].apply(lambda x: round(x, 2))

# %% [markdown]
# ## Extraction json

# %%
weather_fr_data = weather_fr.to_dict(orient='records')
weather_be_data = weather_be.to_dict(orient='records')
infoclimat_data = infoclimat.to_dict(orient='records')

hourly_data =  weather_fr_data + weather_be_data + infoclimat_data

hourly_data_json = json.dumps(hourly_data, indent=4)

s3_file_key = "data_transformed/hourly_data.json"

# Télécharger le fichier sur S3
s3_client.put_object(Body=hourly_data_json, Bucket=bucket_name, Key=s3_file_key)

print(f"Le fichier JSON a été téléchargé sur le bucket S3 : {bucket_name}/{s3_file_key}")
