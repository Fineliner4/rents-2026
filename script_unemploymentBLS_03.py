#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 23 21:23:30 2026

@author: vegagonzalez
"""
#Enlace oficial de la BLS para descargarse datos de unemployment. 
#https://download.bls.gov/pub/time.series/la/la.measure
#Como indica url, 03 = UNEMPLOYMENT. La serie original data.60.metro contiene todos los datos en una misma columna 
#este archivo limpia la columna series_id para quedarse sólo con 03.
#la.data.60.Metro  -  Metropolitan Statistical Areas
#more information regarding the series: https://download.bls.gov/pub/time.series/la/la.txt

import os
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === 1) URL del BLS ===
url = "https://download.bls.gov/pub/time.series/la/la.data.60.Metro"

# === 2) Dónde guardarlo ===
out_dir = "/Users/vegagonzalez/Desktop/rents"
os.makedirs(out_dir, exist_ok=True)

raw_path = os.path.join(out_dir, "la.data.60.Metro.tsv")
csv_path = os.path.join(out_dir, "la.data.60.Metro_ENDS03.csv")

# === 3) Sesión robusta con headers + retries ===
session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://download.bls.gov/"
}

retry = Retry(
    total=5,
    backoff_factor=1.0,
    status_forcelist=[403, 429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

# === 4) Descargar ===
resp = session.get(url, headers=headers, timeout=60, stream=True)
if resp.status_code == 403:
    print("status 403")
    print(dict(resp.headers))
resp.raise_for_status()

with open(raw_path, "wb") as f:
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if chunk:
            f.write(chunk)

print(f"downloaded OK: {raw_path}")

# === 5) Leer + limpiar + filtrar series_id que termina en '03' ===
df = pd.read_csv(raw_path, sep="\t", dtype=str, engine="python")

# Quita espacios en nombres de columnas (series_id -> "series_id")
df.columns = df.columns.str.strip()

# Quita espacios en los valores de series_id 
df["series_id"] = df["series_id"].astype(str).str.strip()
# Quita espacios y convierte año a número para poder filtrar rango
if "year" not in df.columns:
    raise ValueError("La columna 'year' no existe en el archivo descargado.")
df["year"] = pd.to_numeric(df["year"].astype(str).str.strip(), errors="coerce")

# Filtra solo series_id que acaban en "03" y años entre 2012 y 2025
df_03 = df[
    df["series_id"].str.endswith("03", na=False)
    & df["year"].between(2012, 2025, inclusive="both")
].copy()

# Elimina el prefijo de 3 letras (ej. "LAS", "LAU") y el sufijo final "03"
df_03["series_id"] = df_03["series_id"].str[3:-2]

# === 6) Guardar CSV filtrado ===
df_03.to_csv(csv_path, index=False, encoding="utf-8")

print(f"CSV filtrado creado: {csv_path}")
print("Filas filtradas:", len(df_03), "| Columnas:", df_03.shape[1])
print("Series únicas filtradas:", df_03["series_id"].nunique())
print("Años incluidos:", int(df_03["year"].min()) if not df_03.empty else None, "-", int(df_03["year"].max()) if not df_03.empty else None)
print("Ejemplos series_id (sin prefijo de 3 letras ni sufijo 03):", df_03["series_id"].drop_duplicates().head(10).tolist())
