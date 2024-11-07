import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Conexión a MongoDB
client = MongoClient("mongodb://root:example@localhost:27017/")  # Cambia la URL si tu servidor MongoDB es remoto
db = client['weather_database']

# Leer el archivo CSV
#! , dtype={'Country': str} para evitar error de tipos
df = pd.read_csv('data/city_temperature.csv')

# Filtrar datos inválidos
df = df[(df['Year'] >= 1000) & (df['Day'] >= 1) & (df['Day'] <= 31)]

# Crear la columna 'Date' y eliminar filas con fechas inválidas
df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')
df = df.dropna(subset=['Date'])

# Crear y poblar la colección de regiones
region_collection = db['regions']
country_collection = db['countries']
city_collection = db['cities']

# Diccionarios para almacenar las referencias de regiones y países para evitar duplicados
region_ids = {}
country_ids = {}

# Número total de filas para cálculo del progreso
total_rows = len(df)
inserted_count = 0

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
        print(f"Insertando nueva región: {region_name}")
    else:
        region_id = region_ids[region_name]

    # 2. Agregar país si no existe (sin el campo 'state')
    if country_name not in country_ids:
        country = {
            "name": country_name,
            "region_id": region_id
        }
        country_id = country_collection.insert_one(country).inserted_id
        country_ids[country_name] = country_id
        print(f"Insertando nuevo país: {country_name} en la región {region_name}")
    else:
        country_id = country_ids[country_name]

    # 3. Agregar ciudad con estado, fecha y temperatura
    city_data = {
        "name": city_name,
        "state": state_name,  # 'state' ahora se almacena en la colección 'cities'
        "country_id": country_id,
        "date": date,
        "avg_temperature": avg_temperature
    }
    city_collection.insert_one(city_data)

    # Actualizar el conteo de datos insertados y mostrar el porcentaje de progreso
    inserted_count += 1
    if inserted_count % 1000 == 0 or inserted_count == total_rows:
        progress = (inserted_count / total_rows) * 100
        print(f"Progreso: {progress:.2f}% de datos insertados.")

print("Datos insertados correctamente.")
