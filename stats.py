import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Leer el archivo CSV
df = pd.read_csv('data/city_temperature.csv')

# Filtrar datos inválidos
df = df[(df['Year'] >= 1000) & (df['Day'] >= 1) & (df['Day'] <= 31)]

# Crear la columna 'Date' y eliminar filas con fechas inválidas
df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')
df = df.dropna(subset=['Date'])

#? test= df['Country'].unique()
#? print(df.columns)
# Contar el número de países únicos
num_countries = df['Country'].nunique()

# Contar el número de regiones únicas
num_regions = df['Region'].nunique()

# Contar el número de ciudades únicas
num_cities = df['City'].nunique()

# Imprimir los resultados
#? print(f"test: {test}")
print(f"Number of distinct countries: {num_countries}")
print(f"Number of distinct regions: {num_regions}")
print(f"Number of distinct cities: {num_cities}")
