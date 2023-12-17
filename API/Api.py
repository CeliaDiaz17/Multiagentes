from Rates import Rates
""" import Suicidios
import Algoritmo
import Dashboard """
from fastapi import FastAPI, HTTPException, Depends, Response
from fastapi.responses import HTMLResponse

app = FastAPI()


@app.get("/")
def show_options():
    options = [
        ("/rates", "Graficos y estadisticas")
        # ("/suicidios", "Suicidios"),
        # ("/algoritmo", "Algoritmo final"),
        # ("/dashboard", "Cuadro de mandos")
    ]

    formatted_output = ["<h2>Selecciona una opción</h2><ul>"]

    for route, description in options:
        formatted_output.append(
            f"<li><a href='{route}'>{description}</a></li>")

    formatted_output.append("</ul>")

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)


@app.get("/rates")
def show_options_rates():
    options = [
        ("/rates/age_mean/graph", "Edad media de los suicidios por año"),
        ("/rates/sex_stats/graph", "Porcentaje de Mujeres y Hombres suicidados por año"),
        ("/rates/race_stats/graph", "Porcentaje de personas suicidadas por raza y año"),
        ("/rates/age_ranges/graph", "Estadísticas por rangos de edad y por año"),
        ("/rates/unemployment_data/graphs", "Tasas de desempleo"),
        ("/", "Volver al menu")
    ]

    formatted_output = ["<h2>Selecciona una opción</h2><ul>"]

    for route, description in options:
        formatted_output.append(
            f"<li><a href='{route}'>{description}</a></li>")

    formatted_output.append("</ul>")

    result_string = "\n".join(formatted_output)

    return HTMLResponse(content=result_string)


@app.get("/rates/age_mean/stats")
def rate_age_mean_stats():
    rates = Rates()
    return rates.age_mean_stats()


@app.get("/rates/age_mean/graph")
def rate_age_mean_graph():
    rates = Rates()
    return rates.age_mean_graph()


@app.get("/rates/sex_stats/stats")
def rate_sex_stats():
    rates = Rates()
    return rates.sex_mean_stats()


@app.get("/rates/sex_stats/graph")
def rate_sex_stats():
    rates = Rates()
    return rates.sex_stats_graph()


@app.get("/rates/race_stats/stats")
def rate_race_stats():
    rates = Rates()
    return rates.race_stats()


@app.get("/rates/race_stats/graph")
def rate_race_graph():
    rates = Rates()
    return rates.race_graph()


@app.get("/rates/age_ranges/stats")
def rate_age_ranges_data():
    rates = Rates()
    return rates.age_ranges_stats()


@app.get("/rates/age_ranges/graph")
def rate_age_range_graph():
    rates = Rates()
    return rates.age_ranges_graph()


@app.get("/rates/unemployment_data/stats")
def rate_unemployment_stats():
    rates = Rates()
    return rates.unemployment_data()


@app.get("/rates/unemployment_data/graphs")
def rate_unemployment_graph():
    rates = Rates()
    return rates.unemployment_graphs()


""" @app.get("/suicidios")
def suicidios():
    Suicidios.show_options()d

@app.get("/algoritmo")
def algoritmo():
    Algoritmo.show_options()

@app.get("/dashboard")
def dashboard():
    Dashboard.show_options() """

if __name__ == '__main__':
    show_options()
