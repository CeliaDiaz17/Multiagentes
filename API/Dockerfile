# Usar una imagen base de Python. Asegúrate de elegir la versión adecuada.
FROM python:3.8

# Establecer el directorio de trabajo en el contenedor
WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install -r requirements.txt
# Copiar los archivos de tu API al directorio de trabajo.
COPY start.sh .
COPY config.txt .
COPY diccionario.json .
COPY gold /gold
COPY docker_connect.py .
COPY wait-for-it.sh .
COPY Api.py .
COPY Connect.py .
COPY Rates.py .

# Comando para ejecutar la aplicación.

ENTRYPOINT ["./start.sh"]
