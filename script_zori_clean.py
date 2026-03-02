#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 17:12:25 2026

@author: vegagonzalez
"""

# clean_zori_rows.py
import re
from pathlib import Path
import numpy as np
import pandas as pd

INPUT_PATH = Path("/Users/vegagonzalez/Desktop/rents/Metro_zori_uc_sfrcondomfr_sm_sa_month.csv")
MAX_MISSING_PER_ROW = 6

def detect_date_columns(cols):
    """
    Detecta columnas que parecen fechas tipo:
    - 'YYYY-MM-DD' (muy típico en Zillow: fin de mes)
    - 'YYYY-MM'
    """
    date_cols = []
    for c in cols:
        s = str(c).strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s) or re.match(r"^\d{4}-\d{2}$", s):
            date_cols.append(c)
    return date_cols

def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo: {INPUT_PATH}")

    # Lee el CSV: trata cadenas vacías/espacios como NA
    df = pd.read_csv(
        INPUT_PATH,
        low_memory=False,
        na_values=["", " ", "  ", "NA", "N/A", "NaN", "nan", "NULL", "null"],
        keep_default_na=True
    )

    # Quita columnas 100% vacías 
    df = df.dropna(axis=1, how="all")

    # Normaliza "huecos" típicos en columnas de texto (espacios) -> NaN
    # (sin reventar tipos numéricos)
    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) > 0:
        df[obj_cols] = df[obj_cols].replace(r"^\s*$", np.nan, regex=True)

    # Detecta columnas de meses/fechas
    date_cols = detect_date_columns(df.columns)

    # Si no encuentra columnas fecha, aplica el criterio a TODAS las columnas
    cols_for_missing = date_cols if len(date_cols) > 0 else list(df.columns)

    # Columnas "clave" que NO deberían faltar (si existen en tu CSV)
    key_candidates = ["RegionID", "Metro", "RegionName", "SizeRank", "StateName"]
    key_cols = [c for c in key_candidates if c in df.columns]

    # Cuenta huecos por fila (solo en columnas fecha si existen)
    missing_per_row = df[cols_for_missing].isna().sum(axis=1)

    # Regla principal: permitir como máximo 6 huecos por fila
    keep_mask = missing_per_row <= MAX_MISSING_PER_ROW

    # Regla adicional: si hay columnas clave, no permitir que falten
    for kc in key_cols:
        keep_mask &= df[kc].notna()

    df_clean = df.loc[keep_mask].copy()

    # Output
    output_path = INPUT_PATH.with_name(INPUT_PATH.stem + "_cleaned.csv")
    df_clean.to_csv(output_path, index=False)

    # Resumen por consola
    total = len(df)
    kept = len(df_clean)
    dropped = total - kept
    print("=== Limpieza completada ===")
    print(f"Archivo original: {INPUT_PATH}")
    print(f"Filas totales: {total}")
    print(f"Filas mantenidas: {kept}")
    print(f"Filas eliminadas: {dropped}")
    print(f"Criterio: <= {MAX_MISSING_PER_ROW} celdas vacías por fila "
          f"{'(solo en columnas fecha)' if len(date_cols)>0 else '(en todas las columnas)'}")
    if key_cols:
        print(f"Columnas clave exigidas (no vacías): {key_cols}")
    print(f"Guardado en: {output_path}")

if __name__ == "__main__":
    main()