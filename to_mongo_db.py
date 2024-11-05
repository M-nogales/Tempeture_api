import pandas as pd
from pymongo import MongoClient
from datetime import datetime
# Conexión a MongoDB
client = MongoClient("mongodb://root:example@localhost:27017/")  # Cambia la URL si tu servidor MongoDB es remoto
db = client['weather_database']

# Leer el archivo CSV
df = pd.read_csv('data/city_temperature.csv')

# filtrar si hay algún valor de Year con tres dígitos
df = df[df['Year'] >= 1000 & (df['Day'] >= 1) & (df['Day'] <= 31)]

# Verificar una fila en particular

df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')

df = df.dropna(subset=['Date'])

# Crear y poblar la colección de regiones
region_collection = db['regions']
country_collection = db['countries']
city_collection = db['cities']

# Diccionarios para almacenar las referencias de regiones y países para evitar duplicados
region_ids = {}
country_ids = {}

# Iterar por cada fila del DataFrame
for _, row in df.iterrows():
    region_name = row['Region']
    country_name = row['Country']
    state_name = row['State']
    city_name = row['City']
    date = row['Date']
    avg_temperature = row['AvgTemperature']
    
    # 1. Agregar región si no existe
    if region_name not in region_ids:
        region = {"name": region_name}
        region_id = region_collection.insert_one(region).inserted_id
        region_ids[region_name] = region_id
    else:
        region_id = region_ids[region_name]

    # 2. Agregar país y estado si no existe
    country_key = (country_name, state_name)
    if country_key not in country_ids:
        country = {
            "name": country_name,
            "state": state_name,
            "region_id": region_id
        }
        country_id = country_collection.insert_one(country).inserted_id
        country_ids[country_key] = country_id
    else:
        country_id = country_ids[country_key]

    # 3. Agregar ciudad con fecha y temperatura
    city_data = {
        "name": city_name,
        "country_id": country_id,
        "date": date,
        "avg_temperature": avg_temperature
    }
    city_collection.insert_one(city_data)

print("Datos insertados correctamente.")
