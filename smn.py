#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scraper de temperaturas y estaciones meteorológicas

Descarga temperaturas máximas y mínimas, mantiene un acumulado con descargas
pasadas y convierte a series de tiempo.
"""

import os
import re
import sys
import pandas as pd
import arrow
import requests
import json


def get_estaciones(url_estaciones, path_est_backup):
    """Lee y mantiene un backup de estaciones meteorológicas."""

    estaciones = pd.read_fwf(
        url_estaciones, compression="zip", encoding="latin1", skiprows=[1],
        dtype={"FECHA": str}
    )
    try:
        estaciones_backup = pd.read_csv(path_est_backup, encoding="utf8")
        estaciones_total = pd.concat(
            [estaciones, estaciones_backup]).drop_duplicates()
        estaciones_total.to_csv(path_est_backup, encoding="utf8", index=False)
    except Exception as e:
        print(e)
        print("No existía backup de estaciones. Creando el primero...")
        estaciones_total = estaciones
        estaciones_total.to_csv(path_est_backup, encoding="utf8", index=False)
    return estaciones_total


def get_temperaturas(url_temperaturas, path_temp_backup):
    """Lee y mantiene un backup incremental de temperaturas diarias."""

    temperaturas = pd.read_fwf(
        url_temperaturas, compression="zip", encoding="latin1", skiprows=[2],
        header=1, dtype={"FECHA": str})
    try:
        temperaturas_backup = pd.read_csv(path_temp_backup, encoding="utf8")
        temperaturas_total = pd.concat(
            [temperaturas, temperaturas_backup]).drop_duplicates()
        temperaturas_total.to_csv(
            path_temp_backup, encoding="utf8", index=False)
    except Exception as e:
        print(e)
        print("No existía backup de temperaturas. Creando el primero...")
        temperaturas_total = temperaturas
        temperaturas_total.to_csv(
            path_temp_backup, encoding="utf8", index=False)
    return temperaturas_total


def get_temperaturas_estaciones(temperaturas, estaciones):
    temperaturas_estaciones = temperaturas.merge(
        estaciones[["NOMBRE", "NroOACI"]], on="NOMBRE")[
        ["FECHA", "TMAX", "TMIN", "NroOACI"]]
    temperaturas_estaciones["FECHA"] = temperaturas_estaciones["FECHA"].apply(
        lambda x: arrow.get(x, "DDMMYYYY").format("YYYY-MM-DD"))
    return temperaturas_estaciones


def get_unidades_territoriales(latitud, longitud):
    """Busca unidades territoriales a partir de las coordenadas."""
    try:
        latitud = ".".join(re.split("\s+", latitud, maxsplit=2))
        longitud = ".".join(re.split("\s+", longitud, maxsplit=2))

        r = requests.get(
            "https://apis.datos.gob.ar/georef/api/ubicacion?lat={lat}&lon={lon}".format(
                lat=latitud, lon=longitud))
        return r.json()["ubicacion"]

    except Exception as e:
        print(e)
        print("No se pudo localizar lat:{} y lon:{}".format(latitud, longitud))
        return None


def rename_columns(col, prefix):
    if col == "FECHA":
        return "indice_tiempo"
    else:
        return "{}_{}".format(prefix.lower(), col.lower())


def temperatures_panel_to_series(df_panel, field_values="TMAX",
                                 prefix="temperatura_maxima"):
    df_series = df_panel.pivot_table(
        index="FECHA", values=field_values,
        columns="NroOACI"
    ).reset_index().sort_values("FECHA")
    df_series.columns = [
        rename_columns(col, prefix)
        for col in df_series.columns
    ]
    return df_series


def main(config_path="config.json"):

    with open(config_path, "r") as f:
        config = json.load(f)

    url_estaciones = config["URL_ESTACIONES"]
    url_temperaturas = config["URL_TEMPERATURAS"]
    path_est_backup = config["PATH_EST_BACKUP"]
    path_temp_backup = config["PATH_TEMP_BACKUP"]
    path_temp_max = config["PATH_TEMP_MAX"]
    path_temp_min = config["PATH_TEMP_MIN"]
    path_temp_panel = config["PATH_TEMP_PANEL"]
    path_estaciones = config["PATH_ESTACIONES"]

    # crea todos los directorios necesarios
    for path in [path_est_backup, path_temp_backup, path_temp_max,
                 path_temp_min, path_temp_panel, path_estaciones]:
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # 1. LEE Y NORMALIZA ESTACIONES
    # -----------------------------
    print("Leyendo y normalizando estaciones meteorológicas.")
    estaciones = get_estaciones(url_estaciones, path_est_backup)
    estaciones_dict = {
        row[1]["NroOACI"]: row[1]["NOMBRE"]
        for row in estaciones[["NOMBRE", "NroOACI"]].iterrows()
    }
    # extrae ids y nombres oficiales de provincias y departamentos, para cada
    # estación
    print("Agregando unidades territoriales normalizadas.")
    estaciones["api_georef_ubicacion"] = estaciones.apply(
        lambda row: get_unidades_territoriales(
            row["LATITUD"], row["LONGITUD"]),
        axis=1
    )
    estaciones["provincia_id"] = estaciones.api_georef_ubicacion.apply(
        lambda x: x["provincia"]["id"] if x else None)
    estaciones["provincia_nombre"] = estaciones.api_georef_ubicacion.apply(
        lambda x: x["provincia"]["nombre"] if x else None)
    estaciones["departamento_id"] = estaciones.api_georef_ubicacion.apply(
        lambda x: x["departamento"]["id"] if x else None)
    estaciones["departamento_nombre"] = estaciones.api_georef_ubicacion.apply(
        lambda x: x["departamento"]["nombre"] if x else None)

    estaciones_normalizado = estaciones.drop(columns=[
        "api_georef_ubicacion", "PROVINCIA"
    ]).rename(columns={
        "NOMBRE": "estacion_nombre",
        "LATITUD": "estacion_latitud",
        "LONGITUD": "estacion_longitud",
        "ALTURA": "estacion_altura",
        "NRO": "estacion_id",
        "NroOACI": "estacion_oaci_id"
    })

    estaciones_normalizado.to_csv(
        path_estaciones, encoding="utf8", index=False, float_format='%.0f')

    # 2. LEE Y NORMALIZA TEMPERATURAS
    # -----------------------------
    print("Leyendo y normalizando temperaturas.")
    temperaturas = get_temperaturas(url_temperaturas, path_temp_backup)
    temperaturas_estaciones = get_temperaturas_estaciones(
        temperaturas, estaciones
    )

    # crea panel normalizado de temperaturas
    temperaturas_normalizado = temperaturas_estaciones.rename(columns={
        "FECHA": "fecha",
        "TMAX": "temperatura_maxima",
        "TMIN": "temperatura_minima",
        "NroOACI": "estacion_oaci_id"
    })

    temperaturas_normalizado.to_csv(
        path_temp_panel, encoding="utf8", index=False)

    # 3. CONVIERTE TEMPERATURAS A FORMATO DE SERIES
    # -----------------------------
    print("Convirtiendo a series de tiempo.")
    temp_max_series = temperatures_panel_to_series(
        temperaturas_estaciones, "TMAX", "temperatura_maxima")
    temp_min_series = temperatures_panel_to_series(
        temperaturas_estaciones, "TMIN", "temperatura_minima")

    temp_max_series.to_csv(path_temp_max, encoding="utf8", index=False)
    temp_min_series.to_csv(path_temp_min, encoding="utf8", index=False)
    print("Terminado.")


if __name__ == '__main__':
    main(*sys.argv[1:])
