#!/bin/bash
set -e

# Esperar a que la base de datos esté lista
./wait-for-it.sh db:5432 --timeout=30 --strict -- echo "La base de datos está lista."

# Ejecutar el script de Python
echo "Ejecutando docker_connect.py..."
python docker_connect.py

# Iniciar Uvicorn con la aplicación FastAPI
echo "Iniciando Uvicorn..."
exec uvicorn Api:app --host 0.0.0.0 --port 8080
