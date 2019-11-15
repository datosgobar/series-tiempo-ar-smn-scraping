#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Scraper de temperaturas y estaciones meteorológicas

Descarga temperaturas máximas y mínimas, mantiene un acumulado con descargas
pasadas y convierte a series de tiempo.
"""

import os
import sys

URL_ESTACIONES = "https://ssl.smn.gob.ar/dpd/zipopendata.php?dato=estaciones"
URL_TEMPERATURAS = "https://ssl.smn.gob.ar/dpd/zipopendata.php?dato=regtemp"

URL_OUTPUT_TEMP_MIN = "temperaturas-minimas.csv"
URL_OUTPUT_TEMP_MAX = "temperaturas-maximas.csv"


def get_estaciones(url_estaciones):
    estaciones = pd.read_fwf(
        url_estaciones, compression="zip",
        encoding="latin1", skiprows=[1], dtype={"FECHA": str})
    try:
        estaciones_backup = pd.read_csv("estaciones.csv", encoding="utf8")
        estaciones_total = pd.concat(
            [estaciones, estaciones_backup]).drop_duplicates()
        estaciones_total.to_csv("estaciones.csv", encoding="utf8", index=False)
    except:
        estaciones_total = estaciones
    return estaciones_total


def get_temperaturas(url_temperaturas):
    temperaturas = pd.read_fwf(
        url_temperaturas, compression="zip",
        encoding="latin1", skiprows=[2], header=1, dtype={"FECHA": str})
    try:
        temperaturas_backup = pd.read_csv("temperaturas.csv", encoding="utf8")
        temperaturas_total = pd.concat(
            [temperaturas, temperaturas_backup]).drop_duplicates()
        temperaturas_total.to_csv(
            "temperaturas.csv", encoding="utf8", index=False)
    except:
        temperaturas_total = temperaturas
    return temperaturas_total


def get_temperaturas_estaciones(temperaturas, estaciones):
    temperaturas_estaciones = temperaturas.merge(
        estaciones[["NOMBRE", "NroOACI"]], on="NOMBRE"
    )[["FECHA", "TMAX", "TMIN", "NroOACI"]]
    temperaturas_estaciones["FECHA"] = temperaturas_estaciones["FECHA"].apply(
        lambda x: arrow.get(x, "DDMMYYYY").format("YYYY-MM-DD"))
    return temperaturas_estaciones


def rename_columns(col, prefix):
    if col == "FECHA":
        return "indice_tiempo"
    else:
        return "{}_{}".format(prefix.lower(), col.lower())


def temperatures_panel_to_series(df_panel, field_values="TMAX",
                                 prefix="temperatura_maxima"):
    df_series = temperaturas_estaciones.pivot_table(
        index="FECHA", values=field_values,
        columns="NroOACI"
    ).reset_index().sort_values("FECHA")
    df_series.columns = [
        rename_columns(col, prefix)
        for col in df_series.columns
    ]
    return df_series


def main(output_path_tmin=URL_OUTPUT_TEMP_MIN,
         output_path_tmax=URL_OUTPUT_TEMP_MAX,
         url_estaciones=URL_ESTACIONES, url_temperaturas=URL_TEMPERATURAS):
    estaciones = get_estaciones(url_estaciones)
    estaciones_dict = {
        row[1]["NroOACI"]: row[1]["NOMBRE"]
        for row in estaciones[["NOMBRE", "NroOACI"]].iterrows()
    }
    temperaturas = get_temperaturas(url_temperaturas)
    temperaturas_estaciones = get_temperaturas_estaciones(
        temperaturas, estaciones
    )

    temp_max_series = temperatures_panel_to_series(
        temperaturas_estaciones, "TMAX", "temperatura_maxima")
    temp_min_series = temperatures_panel_to_series(
        temperaturas_estaciones, "TMIN", "temperatura_minima")

    temp_min_series.to_csv(output_path_tmin, encoding="utf8", index=False)
    temp_max_series.to_csv(output_path_tmax, encoding="utf8", index=False)


if __name__ == '__main__':
    main(*sys.argv[1:])
