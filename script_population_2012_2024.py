#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 27 13:41:27 2026

@author: vegagonzalez
"""

# -*- coding: utf-8 -*-
"""
Une varios CSV de ACS (B01003: Total population) en un único CSV:
  population_2012_2024.csv

- Lee los archivos desde:
  /Users/vegagonzalez/Desktop/rents/productDownload_2026-02-27T070652

- Añade columna Year extraída del nombre del archivo (2012..2024)
- Mantiene el orden por año (2012→2024)
"""

import os
import re
import pandas as pd

IN_DIR = "/Users/vegagonzalez/Desktop/rents/productDownload_2026-02-27T070652"
OUT_DIR = "/Users/vegagonzalez/Desktop/rents"
OUT_PATH = os.path.join(OUT_DIR, "population_2012_2024.csv")

FILES = [
    "ACSDT1Y2012.B01003-Data",
    "ACSDT1Y2013.B01003-Data",
    "ACSDT1Y2014.B01003-Data",
    "ACSDT1Y2015.B01003-Data",
    "ACSDT1Y2016.B01003-Data",
    "ACSDT1Y2017.B01003-Data",
    "ACSDT1Y2018.B01003-Data",
    "ACSDT1Y2019.B01003-Data",
    "ACSDT5Y2020.B01003-Data",
    "ACSDT1Y2021.B01003-Data",
    "ACSDT1Y2022.B01003-Data",
    "ACSDT1Y2023.B01003-Data",
    "ACSDT1Y2024.B01003-Data",
]

def extract_year(filename: str) -> int:
    m = re.search(r"(20\d{2})", filename)
    if not m:
        raise ValueError(f"No puedo extraer el año del nombre: {filename}")
    return int(m.group(1))

def find_real_path(stem: str) -> str:
    """
    Los downloads de Census a veces guardan como:
      <stem>.csv
    o directamente:
      <stem>
    Probamos ambas.
    """
    candidates = [
        os.path.join(IN_DIR, stem),
        os.path.join(IN_DIR, stem + ".csv"),
        os.path.join(IN_DIR, stem + ".CSV"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"No encuentro el archivo '{stem}' en {IN_DIR} (probé con y sin .csv).")

parts = []

# Ordena por año
FILES_SORTED = sorted(FILES, key=extract_year)

for stem in FILES_SORTED:
    year = extract_year(stem)
    path = find_real_path(stem)

    # ACS suele venir con encabezados y coma; dtype=str para no romper GEO_ID/NAME
    df = pd.read_csv(path, dtype=str)

    # Limpia nombres de columnas (espacios)
    df.columns = [c.strip() for c in df.columns]

    # Inserta Year como primera columna
    df.insert(0, "Year", year)

    parts.append(df)
    print(f" {os.path.basename(path)} | Year={year} | filas={len(df)} | cols={df.shape[1]}")

# Une (concat) respetando el orden por año
out = pd.concat(parts, ignore_index=True)

# En la columna B (segunda columna), deja solo los últimos 5 dígitos
# del valor original. Ejemplo: 310M100US10180 -> 10180
col_b = out.columns[1]
out[col_b] = out[col_b].apply(
    lambda v: re.search(r"(\d{5})$", str(v)).group(1) if pd.notna(v) and re.search(r"(\d{5})$", str(v)) else str(v)[-5:]
)

# Guarda
out.to_csv(OUT_PATH, index=False, encoding="utf-8")

print("\n==============================")
print("OUTPUT creado:")
print(OUT_PATH)
print("Filas totales:", len(out), "| Columnas:", out.shape[1])
print("Primeras columnas:", list(out.columns)[:12])
print("==============================")
