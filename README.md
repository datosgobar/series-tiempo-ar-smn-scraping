# series-tiempo-ar-smn-scraping
Scrapers de datos del Servicio Meteorológico Nacional para su conversión a series de tiempo.

* **scraping-smn-series.ipynb**: notebook donde se explica paso a paso el procesamiento de datos de temperaturas del SMN para su normalización y conversión a series de tiempo.
* **config.example.json**: archivo de configuración de ejemplo, necesario para correr el scraper desde la línea de comandos.

## Instalación

`pip install -r requirements.txt`

## Uso

`python smn.py` Realiza todo el procesamiento descripto en el notebook, y produce los resultados según los parámetros del `config.json`.
