# Obtener la ruta del directorio actual del script en ejecución
$current_dir = $PSScriptRoot

# Verificar si el archivo kaggle.json existe en el directorio de trabajo
if (Test-Path "$current_dir\kaggle.json") {
    # Configurar la variable de entorno KAGGLE_CONFIG_DIR
    $env:KAGGLE_CONFIG_DIR = $current_dir

    Write-Host "Configuración de Kaggle completada. Ruta de kaggle.json: $current_dir"

    # Crear y activar el entorno virtual venv
    python -m venv $current_dir\venv
    . $current_dir\venv\Scripts\Activate.ps1
    Write-Host "Entorno virtual venv creado y activado."

    Start-Sleep -Seconds 7

    # Instalar dependencias desde requirements.txt
    pip install -r "$current_dir\requirements.txt"
    Write-Host "Dependencias instaladas desde requirements.txt."
} else {
    Write-Host "Error: El archivo kaggle.json no se encuentra en el directorio de trabajo."
}
