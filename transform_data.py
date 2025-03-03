# %%
import boto3
import pandas as pd
import numpy as np
from io import StringIO
import json


# %% [markdown]
# # STATIONS

# %%
# Connexion au bucket S3 
bucket_name = "p8-airbyte-greenandcoop"
s3_client = boto3.client("s3")

# Chargement des données
response = s3_client.get_object(Bucket=bucket_name, Key="GreenAndCoop InfoClimat/infoclimat/2025_02_13_1739448920127_0.jsonl")
json_content = response["Body"].read().decode("utf-8")
data = [json.loads(line) for line in json_content.splitlines()]

# Extraire les données imbriquées sous "_airbyte_data" et "stations"
stations = [station for entry in data for station in entry["_airbyte_data"]["stations"]]


# %%
# Transformation des données stations
for doc in stations:
    doc["id_station"] = doc.pop("id")  # Renommer "id" en "id_station"
    doc["city"] = doc["name"]  # Ajouter un champ "city"
    doc["state"] = "France"  # Ajouter un champ "state"
    doc["station_info"] = {
        "name": doc.pop("name"),
        "latitude": doc.pop("latitude"),
        "longitude": doc.pop("longitude"),
        "elevation": doc.pop("elevation"),
        "city": doc.pop("city"),
        "state": doc.pop("state"),
        "type": doc.pop("type"),
        "hardware": doc.get("hardware", None),
        "software": doc.get("software", None),
        "license": {
            "license": doc.get("license", {}).pop("license", None),
            "url": doc.get("license", {}).pop("url", None),
            "source": doc.get("license", {}).pop("source", None),
            "metadonnees": doc.pop("license", {}).pop("metadonnees", None)
        }
    }

# %%
# Ajout des stations Weather Underground
weather_stations = [
    {
        "id_station": "ILAMAD25",
        "station_info": {
            "name": "La Madeleine",
            "latitude": 50.659,
            "longitude": 2.877,
            "elevation": 23,
            "city": "La Madeleine",
            "state": "France",
            "type": None,
            "hardware": "other",
            "software": "EasyWeatherPro_V5.1.6",
            "license": {
                "license": None,
                "url": None,
                "sources": None,
                "metadonnees": None
            }
            
        }
    },
    {
        "id_station": "IICHTE19",
        "station_info": {
            "name": "WeerstationBS",
            "latitude": 51.092,
            "longitude": 2.999,
            "elevation": 15,
            "city": "Ichtegem",
            "state": "Belgium",
            "type": None,
            "hardware": "other",
            "software": "EasyWeatherV1.6.6",
            "license": {
                "license": None,
                "url": None,
                "sources": None,
                "metadonnees": None
            }
        }
    }
]

stations.extend(weather_stations)

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
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    json_content = response["Body"].read().decode("utf-8")
    data = [json.loads(line) for line in json_content.splitlines()]
    return pd.DataFrame([entry["_airbyte_data"] for entry in data])


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

# %%
# Qualité des données : taux de NaN dans les documents

# Concaténer les DataFrames
dfs = [weather_fr, weather_be, infoclimat]
df_hourly_data = pd.concat(dfs, ignore_index=True, join='outer')

def calculate_nan(df):
    '''
    Calcule le nombre et % de NaN dans un df par colonne
    '''
    # Compte les valeurs None
    nan_counts = df.isnull().sum()

    # Compte les valeurs string "NaN", "nan", "none"
    str_nan_counts = df.isin(["NaN", "nan", "none"]).sum()

    # Compte les valeurs vides
    empty_counts = (df == "").sum()

    # Compte les valeurs contenant uniquement des espaces
    space_counts = {}
    for col in (df.select_dtypes(include='object')).columns : 
        space_counts[col] = df[col].str.isspace().sum()
    space_counts = pd.Series(space_counts)

    # Création du DataFrame de toutes les données NaN
    merged_nan_counts = pd.DataFrame({
        'NaN counts': nan_counts.reindex(df.columns, fill_value=0),
        'Str nan counts': str_nan_counts.reindex(df.columns, fill_value=0),
        'Empty counts': empty_counts.reindex(df.columns, fill_value=0),
        'Space counts': space_counts.reindex(df.columns, fill_value=0),
    })



    # Ajout d'une colonne total de NaN
    merged_nan_counts['Total NaN'] = merged_nan_counts.sum(axis=1)

    # Ajout % de NaN
    merged_nan_counts['% NaN'] = round(merged_nan_counts['Total NaN'] / len(df) * 100, 2)

    #Suppression détails NaN counts
    merged_nan_counts = merged_nan_counts.drop(columns=['NaN counts','Str nan counts', 'Empty counts', 'Space counts'])

    # Retourner le DataFrame
    return merged_nan_counts

df_hourly_data_nan = calculate_nan(df_hourly_data)

print("\nNombre de NaN :")
print(df_hourly_data_nan)

# %% [markdown]
# ## Extraction json

# %%
weather_fr_data = weather_fr.to_dict(orient='records')
weather_be_data = weather_be.to_dict(orient='records')
infoclimat_data = infoclimat.to_dict(orient='records')

hourly_data =  weather_fr_data + weather_be_data + infoclimat_data

# %%
# Structurer les données sous "weather_data"
for doc in hourly_data:
    doc["weather_data"] = {
        "temperature": round(doc.pop("temperature", None),2),
        "pressure": doc.pop("pressure", None),
        "humidity": doc.pop("humidity", None),
        "dew_point": round(doc.pop("dew_point", None),2),
        "visibility": doc.pop("visibility", None),
        "wind_speed": round(doc.pop("wind_speed", None),2),
        "wind_gust": doc.pop("wind_gust", None),
        "wind_direction": doc.pop("wind_direction", None),
        "precip_1h": doc.pop("precip_1h", None),
        "precip_3h": doc.pop("precip_3h", None),
        "precip_accum": doc.pop("precip_accum", None),
        "precip_rate": doc.pop("precip_rate", None),
        "solar": doc.pop("solar", None),
        "uv": doc.pop("uv", None),
        "snow_depth": doc.pop("snow_depth", None),
        "nebulosity": doc.pop("nebulosity", None),
        "weather_wmo": doc.pop("weather_wmo", None)
    }

# %%
# Transformer stations en un dictionnaire {id_station: station_info}
stations_dict = {station["id_station"]: station for station in stations}

# Fusionner les données des stations avec les relevés horaires
final_weather_data = []
for record in hourly_data:
    station_info = stations_dict.get(record["id_station"], {}).get("station_info", {})

    final_weather_record = {
        "id_station": record["id_station"],
        "station_info": station_info,
        "datetime": record["datetime"],
        "weather_data": record["weather_data"]
    }
    final_weather_data.append(final_weather_record)

# %%
# Remplacer les NaN par None dans weather_data
for record in final_weather_data:
    for key, value in record["weather_data"].items():
        if isinstance(value, float) and np.isnan(value):  # Vérifier si c'est NaN
            record["weather_data"][key] = None  # Remplacer par None

# %%
weather_data_json = json.dumps(final_weather_data, indent=4)

s3_file_key = "data_transformed/weather_data.json"

# Télécharger le fichier sur S3
s3_client.put_object(Body=weather_data_json, Bucket=bucket_name, Key=s3_file_key)

print(f"\nLe fichier weather_data JSON a été téléchargé sur le bucket S3 : {bucket_name}/{s3_file_key}")
print(f"Il contient: {len(final_weather_data)} documents")