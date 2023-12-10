from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
import oracledb
import traceback
import sys
import getpass
from fastapi.responses import JSONResponse
from Connect import Connect
from fastapi.responses import HTMLResponse
import json

# FastAPI
app = FastAPI()

# Modelos Pydantic para las operaciones CRUD
class ItemCreate(BaseModel):
    name: str
    description: str

class ItemUpdate(ItemCreate):
    pass

class ItemResponse(ItemCreate):
    id: int


@app.get("/rates")
def show_options():
    options = [
        ("/rates/age_mean", "Edad media de los suicidios por año"),
        ("/rates/sex_stats", "Porcentaje de Mujeres y Hombres suicidados por año"),
        ("/rates/race_stats", "Porcentaje de personas suicidadas por raza y año"),
        ("/rates/age_stats", "Estadísticas por edad y por año")
    ]

    formatted_output = ["<h2>Selecciona una opción</h2><ul>"]

    for route, description in options:
        formatted_output.append(f"<li><a href='{route}'>{description}</a></li>")

    formatted_output.append("</ul>")

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)

@app.get("/rates/age_mean")
def calculate_age_mean():
    # Inicia una sesión de la base de datos
    db = Connect()

    # Realiza la consulta para obtener la media de edad por año
    query_result = db.execute_query("SELECT current_data_year, AVG(age_recode_21) as mean_age FROM SCHEMA_GOLD.gold_mortalidad_data GROUP BY current_data_year")

    # Formatea los resultados de la consulta
    age_means_by_year = {row[0]: {"mean_age": row[1]} for row in query_result}
    sorted_age_means_by_year = dict(sorted(age_means_by_year.items()))

    message = {"message": "MEDIA DE EDAD POR AÑO", "age_means_by_year": sorted_age_means_by_year}

    formatted_output = [f"<h2>{message['message']}</h2>"]

    for year, data in message["age_means_by_year"].items():
        formatted_output.append(f"<p style='margin: 1px;'><strong>Año {year}:</strong></p>")
        formatted_output.append(f"<p style='margin: 1px;'>- Edad media = {data['mean_age']:.2f}</p>")
        formatted_output.append("<br>")  # Utiliza <br> para los saltos de línea

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)


@app.get("/rates/sex_stats")
def calculate_sex_mean():
    # Inicia una sesión de la base de datos
    db = Connect()

    # Realiza la consulta para obtener el porcentaje de mujeres y hombres por año
    query_result = db.execute_query("SELECT current_data_year, SUM(CASE WHEN sex='F' THEN 1 ELSE 0 END) as female_count, SUM(CASE WHEN sex='M' THEN 1 ELSE 0 END) as male_count, COUNT(*) as total_count FROM SCHEMA_GOLD.gold_mortalidad_data GROUP BY current_data_year")

    # Procesa los resultados de la consulta para calcular los porcentajes
    sex_stats_by_year = {}
    for row in query_result:
        year = row[0]
        female_count = row[1]
        male_count = row[2]
        total_count = row[3]
        female_percentage = (female_count / total_count) * 100.0
        male_percentage = (male_count / total_count) * 100.0
        sex_stats_by_year[year] = {"female_percentage": female_percentage, "male_percentage": male_percentage}

    message = {"message": "Porcentaje de mujeres y hombres por año", "sex_stats_by_year": sex_stats_by_year}
    message["sex_stats_by_year"] = {str(year): values for year, values in sorted(message["sex_stats_by_year"].items())}

    formatted_output = [f"<h2>{message['message']}</h2>"]

    for year, percentages in message["sex_stats_by_year"].items():
        formatted_output.append(f"<p style='margin: 1px;'><strong>{year}:</strong></p>")
        formatted_output.append(f"<p style='margin: 1px;'>- Hombres = {percentages['male_percentage']:.2f}%</p>")
        formatted_output.append(f"<p style='margin: 1px;'>- Mujeres = {percentages['female_percentage']:.2f}%</p>")
        formatted_output.append("<br>")  # Utiliza <br> para los saltos de línea

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)

@app.get("/rates/race_stats")
def calculate_race_stats():
    # Inicia una sesión de la base de datos
    db = Connect()

    # Realiza la consulta para obtener el conteo de instancias por raza y año
    query_result = db.execute_query("""
        SELECT current_data_year, race, COUNT(*) as count_per_race
        FROM SCHEMA_GOLD.gold_mortalidad_data
        GROUP BY current_data_year, race
    """)

    # Abre el archivo diccionario.json y carga el contenido
    with open('diccionario.json', 'r') as file:
        race_mapping  = json.load(file).get('root', {}).get('race', {})

    # Procesa los resultados de la consulta para calcular los porcentajes
    race_stats_by_year = {}
    for row in query_result:
        year = row[0]
        race_numeric = row[1]
        count_per_race = row[2]

        if year not in race_stats_by_year:
            race_stats_by_year[year] = {}

        race_name = race_mapping.get(str(race_numeric))

        # Asegúrate de que race_name no sea None
        if race_name is not None:
            # Convierte race_name a cadena para garantizar que sea un tipo de datos compatible
            race_name = str(race_name)

            # Agrega el conteo al diccionario
            race_stats_by_year[year][race_name] = count_per_race


    # Calcula el porcentaje de instancias por raza por año
    race_percentage_by_year = {}
    for year, race_counts in race_stats_by_year.items():
        total_instances = sum(race_counts.values())
        race_percentage_by_year[year] = {race: (count / total_instances) * 100.0 for race, count in race_counts.items()}

    # Ordena el diccionario por año (de más antiguo a más nuevo)
    sorted_race_percentage_by_year = dict(sorted(race_percentage_by_year.items()))

    formatted_output = [f"<h2>Porcentaje por raza</h2>"]

    for year, race_percentages in sorted_race_percentage_by_year.items():
        formatted_output.append(f"<p><strong>{year}:</strong></p>")
        for race, percentage in race_percentages.items():
            formatted_output.append(f"<p>{race}: {percentage:.2f}%</p>")

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)


@app.get("/rates/age_stats")
def calculate_age_stats():
    # Inicia una sesión de la base de datos
    db = Connect()

    # Realiza la consulta para obtener el conteo de instancias por edad y año
    query_result = db.execute_query("""
        SELECT current_data_year, age_recode_21, COUNT(*) as count_per_age
        FROM SCHEMA_GOLD.gold_mortalidad_data
        GROUP BY current_data_year, age_recode_21
    """)

    # Procesa los resultados de la consulta para calcular los porcentajes
    age_stats_by_year = {}
    for row in query_result:
        year = row[0]
        age = row[1]
        count_per_age = row[2]

        if year not in age_stats_by_year:
            age_stats_by_year[year] = {"0-20": 0, "20-40": 0, "40-60": 0, "60-80": 0, "80+": 0}

        # Clasifica las edades en los rangos correspondientes
        if age < 20:
            age_stats_by_year[year]["0-20"] += count_per_age
        elif age < 40:
            age_stats_by_year[year]["20-40"] += count_per_age
        elif age < 60:
            age_stats_by_year[year]["40-60"] += count_per_age
        elif age < 80:
            age_stats_by_year[year]["60-80"] += count_per_age
        else:
            age_stats_by_year[year]["80+"] += count_per_age

    # Calcula el porcentaje de instancias por edad por año
    age_percentage_by_year = {}
    for year, age_counts in age_stats_by_year.items():
        total_instances = sum(age_counts.values())
        age_percentage_by_year[year] = {age_range: (count / total_instances) * 100.0 for age_range, count in age_counts.items()}

    formatted_output = [f"<h2>Porcentaje por edad</h2>"]

    for year, age_percentages in age_percentage_by_year.items():
        formatted_output.append(f"<p><strong>{year}:</strong></p>")
        for age_range, percentage in age_percentages.items():
            formatted_output.append(f"<p>{age_range} años: {percentage:.2f}%</p>")

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)

@app.get("/rates/suicide_rates")
def calculate_suicide_rates():
    # Inicia una sesión de la base de datos
    db = Connect()

    # Realiza la consulta para obtener la media del RATE por año
    query_result = db.execute_query("""
        SELECT YEAR, AVG(RATE) as mean_rate
        FROM SCHEMA_GOLD.gold_suicide_rate_data
        GROUP BY YEAR
    """)

    # Procesa los resultados de la consulta
    suicide_rates_by_year = {row[0]: row[1] for row in query_result}

    formatted_output = [f"<h2>Media de Tasa de Suicidios por Año</h2>"]

    # Ordena los años de menor a mayor
    sorted_suicide_rates = dict(sorted(suicide_rates_by_year.items()))

    for year, mean_rate in sorted_suicide_rates.items():
        formatted_output.append(f"<p><strong>{year}:</strong> Media de tasa = {mean_rate:.2f}</p>")

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)

""" @app.get("/{variable1}/{variable2}")
def dynamic_route(variable1: str, variable2: str):
    # Realizar acciones en función de las variables recibidas
    result = {"variable1": variable1, "variable2": variable2}
    return result  """