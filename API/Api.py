from Rates import Rates
""" import Suicidios
import Algoritmo
import Dashboard """
from fastapi import FastAPI, HTTPException, Depends, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.responses import JSONResponse

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],)


@app.get("/", response_class=JSONResponse)
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

    return JSONResponse(content=result_string)


@app.get("/rates", response_class=JSONResponse)
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

    return JSONResponse(content=result_string)


@app.get("/rates/age_mean")
def rate_age_mean():
    rates = Rates()
    return rates.create_msg_age_mean()


@app.get("/rates/age_mean/stats", response_class=JSONResponse)
def rate_age_mean_stats():
    rates = Rates()
    return rates.age_mean_stats()


@app.get("/rates/age_mean/graph", response_class=JSONResponse)
def rate_age_mean_graph():
    rates = Rates()
    return rates.age_mean_graph()


@app.get("/rates/sex_stats/stats", response_class=JSONResponse)
def rate_sex_stats():
    rates = Rates()
    return rates.sex_mean_stats()


@app.get("/rates/sex_stats/graph", response_class=JSONResponse)
def rate_sex_stats():
    rates = Rates()
    return rates.sex_stats_graph()


@app.get("/rates/race_stats/stats", response_class=JSONResponse)
def rate_race_stats():
    rates = Rates()
    return rates.race_stats()


@app.get("/rates/race_stats/graph", response_class=JSONResponse)
def rate_race_graph():
    rates = Rates()
    return rates.race_graph()


@app.get("/rates/age_ranges/stats", response_class=JSONResponse)
def rate_age_ranges_data():
    rates = Rates()
    return rates.age_ranges_stats()


@app.get("/rates/age_ranges/graph", response_class=JSONResponse)
def rate_age_range_graph():
    rates = Rates()
    return rates.age_ranges_graph()


@app.get("/rates/unemployment_data/stats", response_class=JSONResponse)
def rate_unemployment_stats():
    rates = Rates()
    return rates.unemployment_data()


@app.get("/rates/unemployment_data/graphs", response_class=JSONResponse)
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
