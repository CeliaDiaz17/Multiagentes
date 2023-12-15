from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
import oracledb
import traceback
import sys
import getpass
from fastapi.responses import JSONResponse
from Connect import Connect
from fastapi.responses import JSONResponse
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

# Modelos Pydantic para las operaciones CRUD


class ItemCreate(BaseModel):
    name: str
    description: str


class ItemUpdate(ItemCreate):
    pass


class ItemResponse(ItemCreate):
    id: int


class Rates:

    def age_mean_stats(self):
        data = self.create_msg_age_mean()
        sorted_age_means_by_year = data[0]
        message = data[1]

        menu_link = "/rates"
        graph_link = "/rates/age_mean/graph"

        formatted_output = [
            f"<h2>{message['message']}</h2>",
            f"<p><a href='{graph_link}'>Ver Gráfico de Edad Media</a></p>",
        ]

        for year, data in message["age_means_by_year"].items():
            formatted_output.append(
                f"<p style='margin: 1px;'><strong>Año {year}:</strong></p>")
            formatted_output.append(
                f"<p style='margin: 1px;'>- Edad media = {data['mean_age']:.2f}</p>")
            # Utiliza <br> para los saltos de línea
            formatted_output.append("<br>")

        formatted_output.append(
            f"<p><a href='{menu_link}'>Volver al menu</a></p>")
        result_string = "\n".join(formatted_output)

        return JSONResponse(content=result_string)

    def age_mean_graph(self):
        data = self.create_msg_age_mean()
        sorted_age_means_by_year = data[0]
        message = data[1]

        graph_link = "/rates/age_mean/stats"
        menu_link = "/rates"
        # Crear el gráfico
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=list(sorted_age_means_by_year.keys()), y=[
                      data['mean_age'] for data in sorted_age_means_by_year.values()], mode='lines+markers', name='Edad Media'))

        # Configurar el diseño del gráfico
        fig.update_layout(title='Media de Edad por Año',
                          xaxis_title='Año', yaxis_title='Edad Media')

        # Convertir el gráfico a HTML
        graph_html = fig.to_html(full_html=False)

        # Formatear el resultado con el gráfico
        formatted_output = [
            f"<h2>{message['message']}</h2>",
            f"<p><a href='{graph_link}'>Estadisticas de edad media</a></p>",
            f"<div>{graph_html}</div>"
            f"<p><a href='{menu_link}'>Volver al menu</a></p>",
        ]

        result_string = "\n".join(formatted_output)

        return JSONResponse(content=result_string)

    def create_msg_age_mean(self):
        db = Connect()

        # Realiza la consulta para obtener la media de edad por año
        query_result = db.execute_query(
            "SELECT current_data_year, AVG(age_recode_21) as mean_age FROM SCHEMA_GOLD.gold_mortalidad_data GROUP BY current_data_year")

        # Formatea los resultados de la consulta
        age_means_by_year = [{"year": row[0], "mean_age": row[1]}
                             for row in query_result]

        message = {"message": "MEDIA DE EDAD POR AÑO",
                   "age_means_by_year": age_means_by_year}

        return message

    def sex_mean_stats(self):
        message = self.create_msg_sex_stats()

        menu_link = "/rates"
        graph_link = "/rates/sex_stats/graph"
        formatted_output = [
            f"<h2>{message['message']}</h2>",
            f"<a href={graph_link}>Ver gráfico</a></p>"
        ]

        for year, percentages in message["sex_stats_by_year"].items():
            formatted_output.append(
                f"<p style='margin: 1px;'><strong>{year}:</strong></p>")
            formatted_output.append(
                f"<p style='margin: 1px;'>- Hombres = {percentages['male_percentage']:.2f}%</p>")
            formatted_output.append(
                f"<p style='margin: 1px;'>- Mujeres = {percentages['female_percentage']:.2f}%</p>")
            # Utiliza <br> para los saltos de línea
            formatted_output.append("<br>")

        formatted_output.append(
            f"<p><a href='{menu_link}'>Volver al menu</a></p>")
        result_string = "\n".join(formatted_output)

        return JSONResponse(content=result_string)

    def sex_stats_graph(self):
        # Lógica para obtener los datos
        data = self.create_msg_sex_stats()

        # Convertir las claves a una lista
        years = list(data["sex_stats_by_year"].keys())

        # Crear el DataFrame
        # Transponer para tener años como índice
        df = pd.DataFrame(data["sex_stats_by_year"]).T

        # Crear el gráfico de líneas para mujeres y hombres en el mismo gráfico
        fig_combined = px.line(
            df,
            x=df.index,
            y=["female_percentage", "male_percentage"],
            title="Porcentaje de Mujeres y Hombres por año",
            labels={"value": "Porcentaje"},
            color_discrete_map={
                "female_percentage": "#FF00FF", "male_percentage": "blue"},
            line_shape="linear"
        )

        # Dentro de la función sex_stats_graph
        html_content = """
        <h1>Porcentaje de Mujeres y Hombres por año</h1>
        <p><a href="/rates/sex_stats/stats">Ver estadísticas detalladas</a></p>
        """  # Título y enlace a las estadísticas detalladas

        # Agregar el HTML del gráfico
        html_content += fig_combined.to_html(full_html=False)

        menu_link = '/rates'
        # Agregar el enlace para volver al menú al final
        html_content += f"<p><a href='{menu_link}'>Volver al menú</a></p>"

        # Devolver el HTML actualizado
        return JSONResponse(content=html_content)

    def create_msg_sex_stats(self):
        # Inicia una sesión de la base de datos
        db = Connect()

        # Realiza la consulta para obtener el porcentaje de mujeres y hombres por año
        query_result = db.execute_query(
            "SELECT current_data_year, SUM(CASE WHEN sex='F' THEN 1 ELSE 0 END) as female_count, SUM(CASE WHEN sex='M' THEN 1 ELSE 0 END) as male_count, COUNT(*) as total_count FROM SCHEMA_GOLD.gold_mortalidad_data GROUP BY current_data_year")

        # Procesa los resultados de la consulta para calcular los porcentajes
        sex_stats_by_year = {}
        for row in query_result:
            year = row[0]
            female_count = row[1]
            male_count = row[2]
            total_count = row[3]
            female_percentage = (female_count / total_count) * 100.0
            male_percentage = (male_count / total_count) * 100.0
            sex_stats_by_year[year] = {
                "female_percentage": female_percentage, "male_percentage": male_percentage}

        message = {"message": "Porcentaje de mujeres y hombres por año",
                   "sex_stats_by_year": sex_stats_by_year}
        message["sex_stats_by_year"] = {str(year): values for year, values in sorted(
            message["sex_stats_by_year"].items())}

        return message

    def race_stats(self):
        sorted_race_percentage_by_year = self.create_msg_race()

        # Agrega el enlace al gráfico
        graph_link = '<a href="/rates/race_stats/graph">Ver gráfico</a>'
        formatted_output = [f"<h2>Porcentaje por raza</h2>{graph_link}"]

        for year, race_percentages in sorted_race_percentage_by_year.items():
            formatted_output.append(f"<p><strong>{year}:</strong></p>")
            for race, percentage in race_percentages.items():
                formatted_output.append(f"<p>{race}: {percentage:.2f}%</p>")

        menu_link = '/rates'
        formatted_output.append(
            f"<p><a href='{menu_link}'>Volver al menu</a></p>")
        result_string = "\n".join(formatted_output)

        return JSONResponse(content=result_string)

    def race_graph(self):
        # Lógica para obtener los datos
        data = self.create_msg_race()

        # Convertir los datos a un formato adecuado para Plotly Express
        years = []
        races = []
        percentages = []

        for year, race_percentages in data.items():
            for race, percentage in race_percentages.items():
                years.append(year)
                races.append(race)
                percentages.append(percentage)

        # Crear el gráfico de puntos unidos por línea
        fig = px.line(
            x=years,
            y=percentages,
            color=races,
            labels={"x": "Año", "y": "Porcentaje"},
            title="Porcentaje de instancias por raza y año",
        )

        # Agregar la línea gris discontinua en y=14.28
        fig.add_shape(
            go.layout.Shape(
                type="line",
                # Puedes ajustar estos valores según tus necesidades
                x0=min(years),
                x1=max(years),
                y0=14.28,
                y1=14.28,
                line=dict(color="gray", dash="dash"),
            )
        )
        # Obtener el HTML del gráfico
        html_content = fig.to_html(full_html=False)

        # Agregar el título y los enlaces al HTML
        stats_link = '/rates/race_stats/stats'
        menu_link = '/rates'
        html_content = f"<h1>Porcentaje de instancias por raza y año</h1><p><a href='{stats_link}'>Ver estadísticas</a></p>" + html_content
        # Agregar el enlace para volver al menú al final
        html_content += f"<p><a href='{menu_link}'>Volver al menú</a></p>"

        # Devolver el HTML actualizado
        return JSONResponse(content=html_content)

    def create_msg_race(self):
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
            race_mapping = json.load(file).get('root', {}).get('race', {})

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
            race_percentage_by_year[year] = {
                race: (count / total_instances) * 100.0 for race, count in race_counts.items()}

        # Ordena el diccionario por año (de más antiguo a más nuevo)
        sorted_race_percentage_by_year = dict(
            sorted(race_percentage_by_year.items()))

        return sorted_race_percentage_by_year

    def age_ranges_stats(self):
        sorted_age_percentage_by_year = self.create_msg_age_ranges()

        # Agrega el enlace al gráfico
        graph_link = '<a href="/rates/age_ranges/graph">Ver gráfico</a>'
        formatted_output = [
            f"<h2>Porcentaje por rango de edad</h2>{graph_link}"]

        for year, age_percentages in sorted_age_percentage_by_year.items():
            formatted_output.append(f"<p><strong>{year}:</strong></p>")
            for age_range, percentage in age_percentages.items():
                formatted_output.append(
                    f"<p>{age_range}: {percentage:.2f}%</p>")

        menu_link = '/rates'
        formatted_output.append(
            f"<p><a href='{menu_link}'>Volver al menu</a></p>")
        result_string = "\n".join(formatted_output)

        return JSONResponse(content=result_string)

    def age_ranges_graph(self):
        # Lógica para obtener los datos
        data = self.create_msg_age_ranges()

        # Convertir los datos a un formato adecuado para Plotly Express
        years = []
        age_ranges = []
        percentages = []

        for year, age_percentages in data.items():
            for age_range, percentage in age_percentages.items():
                years.append(year)
                age_ranges.append(age_range)
                percentages.append(percentage)

        # Crear el gráfico de barras apiladas
        fig = px.bar(
            x=years,
            y=percentages,
            color=age_ranges,
            labels={"x": "Año", "y": "Porcentaje"},
            title="Porcentaje de instancias por rango de edad y año",
        )

        # Obtener el HTML del gráfico
        html_content = fig.to_html(full_html=False)

        # Agregar el enlace a las estadísticas y el enlace para volver al menú al HTML
        stats_link = '/rates/age_ranges/stats'
        menu_link = '/rates'
        html_content = f"<h1>Porcentaje de instancias por rango de edad y año</h1><p><a href='{stats_link}'>Ver estadísticas</a></p>" + html_content
        # Agregar el enlace para volver al menú al final
        html_content += f"<p><a href='{menu_link}'>Volver al menú</a></p>"

        # Devolver el HTML actualizado
        return JSONResponse(content=html_content)

    def create_msg_age_ranges(self):
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
                age_stats_by_year[year] = {
                    "0-20": 0, "20-40": 0, "40-60": 0, "60-80": 0, "80+": 0}

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
            age_percentage_by_year[year] = {age_range: (
                count / total_instances) * 100.0 for age_range, count in age_counts.items()}

        return age_percentage_by_year

    def unemployment_data(self):
        sorted_unemployment_data = self.create_msg_unemployment()

        graph_link = '<a href="/rates/unemployment_data/graphs">Ver gráfico</a>'

        # Formatea los resultados en HTML
        formatted_output = [
            f"<h2>Estadísticas de desempleo por año</h2>{graph_link}"]

        for year, stats in sorted_unemployment_data.items():
            formatted_output.append(f"<p><strong>{year}:</strong></p>")
            formatted_output.append(
                f"<p>Hombres: {stats['mean_men']:.2f}%</p>")
            formatted_output.append(
                f"<p>Mujeres: {stats['mean_women']:.2f}%</p>")
            formatted_output.append(
                f"<p>Primary school: {stats['primary_school']:.2f}%</p>")
            formatted_output.append(
                f"<p>High school: {stats['high_school']:.2f}%</p>")
            formatted_output.append(
                f"<p>Associates degree: {stats['associates_degree']:.2f}%</p>")
            formatted_output.append(
                f"<p>Professional degree: {stats['professional_degree']:.2f}%</p>")
            formatted_output.append(f"Blancos: {stats['white']:.2f}%</p>")
            formatted_output.append(
                f"<p>Racializados: {stats['mean_ethnicity']:.2f}%</p>")
            formatted_output.append(
                f"<p>TOTAL: {stats['total_mean']:.2f}%</p>")
            # Agrega más líneas según sea necesario

        menu_link = '/rates'
        formatted_output.append(
            f"<p><a href='{menu_link}'>Volver al menu</a></p>")
        result_string = "\n".join(formatted_output)

        return JSONResponse(content=result_string)

    def unemployment_graphs(self):
        data = self.create_msg_unemployment()

        # Convertir los datos a un formato adecuado para Plotly Express
        years = []
        men_values = []
        women_values = []
        primary_school_values = []
        high_school_values = []
        associates_degree_values = []
        professional_degree_values = []
        white_values = []
        ethnicity_values = []

        for year, values in data.items():
            years.append(year)
            men_values.append(values["mean_men"])
            women_values.append(values["mean_women"])
            primary_school_values.append(values["primary_school"])
            high_school_values.append(values["high_school"])
            associates_degree_values.append(values["associates_degree"])
            professional_degree_values.append(values["professional_degree"])
            white_values.append(values["white"])
            ethnicity_values.append(values["mean_ethnicity"])

        # Crear los tres gráficos de puntos unidos por línea
        fig_men_women = px.line(
            x=years,
            y=[men_values, women_values],
            color_discrete_sequence=["blue", "pink"],
            labels={"x": "Año", "y": "Porcentaje", "color": "Género"},
            title="Tasa de desempleo para Hombres y Mujeres por año",
        )
        fig_men_women.for_each_trace(lambda t: t.update(
            name="Hombres") if t.name == "wide_variable_0" else t.update(name="Mujeres"))

        fig_education = px.line(
            x=years,
            y=[primary_school_values, high_school_values,
                associates_degree_values, professional_degree_values],
            color_discrete_sequence=["green", "orange", "red", "purple"],
            labels={"x": "Año", "y": "Porcentaje", "color": "Nivel educativo"},
            title="Tasa de desempleo por nivel educativo por año",
        )
        fig_education.for_each_trace(lambda t: t.update(name="Primaria") if t.name == "wide_variable_0" else t.update(name="Secundaria") if t.name == "wide_variable_1" else t.update(
            name="Técnico") if t.name == "wide_variable_2" else t.update(name="Profesional") if t.name == "wide_variable_3" else None)

        fig_ethnicity = px.line(
            x=years,
            y=[white_values, ethnicity_values],
            color_discrete_sequence=["gray", "brown"],
            labels={"x": "Año", "y": "Porcentaje", "color": "Etnia"},
            title="Tasa de desempleo por etnia por año",
        )
        fig_ethnicity.for_each_trace(lambda t: t.update(name="Blancos") if t.name == "wide_variable_0" else t.update(
            name="Racializados") if t.name == "wide_variable_1" else None)

        # Combina los gráficos en un solo HTML
        combined_html = f"{fig_men_women.to_html(full_html=False)}<br>{fig_education.to_html(full_html=False)}<br>{fig_ethnicity.to_html(full_html=False)}"

        # Agregar el enlace a las estadísticas y el enlace para volver al menú al HTML
        stats_link = '/rates/unemployment_data/stats'
        menu_link = '/rates'
        combined_html = f"<h1>Tasa de desempleo</h1><p><a href='{stats_link}'>Ver estadísticas</a></p>" + combined_html
        # Agregar el enlace para volver al menú al final
        combined_html += f"<p><a href='{menu_link}'>Volver al menú</a></p>"

        # Devuelve los gráficos combinados en formato HTML
        return JSONResponse(content=combined_html)

    def create_msg_unemployment(self):
        db = Connect()

        query_result = db.execute_query("""
            SELECT
                YEAR,
                AVG(MEN) AS MEN,
                AVG(WOMEN) AS WOMEN,
                AVG(PRIMARY_SCHOOL) AS PRIMARY_SCHOOL,
                AVG(HIGH_SCHOOL) AS HIGH_SCHOOL,
                AVG(ASSOCIATES_DEGREE) AS ASSOCIATES_DEGREE,
                AVG(PROFESSIONAL_DEGREE) AS PROFESSIONAL_DEGREE,
                AVG(WHITE) AS WHITE,
                AVG(BLACK) AS BLACK,
                AVG(ASIAN) AS ASIAN,
                AVG(HISPANIC) AS HISPANIC
            FROM
                SCHEMA_GOLD.gold_suicide_rate_and_unemployment_data
            GROUP BY
                YEAR""")

        stats_by_year = {}
        for row in query_result:
            year = row[0]
            if any(value is None for value in row[1:]):
                continue
            men = row[1]
            women = row[2]
            primary_school = row[3]
            high_school = row[4]
            associates_degree = row[5]
            professional_degree = row[6]
            white = row[7]
            black = row[8]
            asian = row[9]
            hispanic = row[10]

            # Realiza cálculos adicionales según sea necesario
            total = primary_school + high_school + associates_degree + \
                professional_degree + white + black + asian + hispanic + men + women
            total_ethnicity = black + asian + hispanic

            # Agrega los resultados a un diccionario
            stats_by_year[year] = {
                "mean_men": men,
                "mean_women": women,
                "primary_school": primary_school,
                "high_school": high_school,
                "associates_degree": associates_degree,
                "professional_degree": professional_degree,
                "white": white,
                "mean_ethnicity": total_ethnicity / 4,  # Promedio de la etnia
                "total_mean": total/10
                # Agrega más cálculos según sea necesario
            }

        sorted_unemployment_data = dict(sorted(stats_by_year.items()))

        return sorted_unemployment_data
