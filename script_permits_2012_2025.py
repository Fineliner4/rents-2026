#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 26 10:06:18 2026

@author: vegagonzalez
"""

# -*- coding: utf-8 -*-
"""
Une tb3u2012–tb3u2018 (TXT Census) + Excels locales 2019–2025 (hojas "Units")
y genera un único CSV en:
  /Users/vegagonzalez/Desktop/rents/permits_cbsa_2012_2025.csv

Columnas (orden fijo + Year):
Year, CSA, CBSA, Name, Total, 1 Unit, 2 Units, 3 and 4 Units, 5 Units or More,
Num of Structures With 5 Units or More
"""

import os
import re
import pandas as pd
import requests

# =========================
# RUTA BASE
# =========================
BASE_DIR = "/Users/vegagonzalez/Desktop/rents"
os.makedirs(BASE_DIR, exist_ok=True)

# =========================
# COLUMNAS OUTPUT (orden fijo + Year)
# =========================
EXPECTED_COLS = [
    "Year",
    "CSA",
    "CBSA",
    "Name",
    "Total",
    "1 Unit",
    "2 Units",
    "3 and 4 Units",
    "5 Units or More",
    "Num of Structures With 5 Units or More",
]

# =========================
# EXCELS LOCALES (2019–2025)
# =========================
EXCEL_FILES = [
    "msaannual_201999",
    "msaannual_202099",
    "msaannual_202199",
    "msaannual_202299",
    "msaannual_202399",
    "cbsaannual_202499",
    "cbsaannual_2025prelim",
]

# Mapea stem -> año (via nombre file)
def year_from_stem(stem: str) -> int:
    m = re.search(r"(20\d{2})", stem)
    if not m:
        raise ValueError(f"No puedo extraer año del nombre: {stem}")
    return int(m.group(1))

# =========================
# TXTs (2012–2018)
# =========================
TXT_URLS = {
    2012: "https://www.census.gov/construction/bps/txt/tb3u2012.txt",
    2013: "https://www.census.gov/construction/bps/txt/tb3u2013.txt",
    2014: "https://www.census.gov/construction/bps/txt/tb3u2014.txt",
    2015: "https://www.census.gov/construction/bps/txt/tb3u2015.txt",
    2016: "https://www.census.gov/construction/bps/txt/tb3u2016.txt",
    2017: "https://www.census.gov/construction/bps/txt/tb3u2017.txt",
    2018: "https://www.census.gov/construction/bps/txt/tb3u2018.txt",
}


# =========================
# HELPERS
# =========================
def find_existing_excel_path(stem: str) -> str:
    """Busca el archivo Excel en BASE_DIR probando extensiones comunes."""
    for ext in [".xlsx", ".xls", ".xlsm"]:
        p = os.path.join(BASE_DIR, stem + ext)
        if os.path.exists(p):
            return p
    p = os.path.join(BASE_DIR, stem)
    if os.path.exists(p):
        return p
    raise FileNotFoundError(
        f"No encuentro el Excel '{stem}' en {BASE_DIR} con extensiones .xlsx/.xls/.xlsm."
    )


def _clean_colname(c: str) -> str:
    c = str(c)
    c = re.sub(r"\s+", " ", c).strip()
    return c


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas de Excels/TXTs a las columnas esperadas (sin Year).
    MUY tolerante: detecta columnas por substring.
    """
    df = df.copy()
    df.columns = [_clean_colname(c) for c in df.columns]

    # Detectores por substring
    def find_col(substrs):
        for c in df.columns:
            cl = c.lower()
            ok = all(s in cl for s in substrs)
            if ok:
                return c
        return None

    # Prioriza nombres exactos/frecuentes (evita confundir columnas con años/notas)
    lower_to_real = {c.lower(): c for c in df.columns}
    exact_candidates = {
        "CSA": ["csa"],
        "CBSA": ["cbsa", "cbsa code"],
        "Name": ["name", "cbsa name", "metropolitan statistical area"],
        "Total": ["total", "total units"],
        "1 Unit": ["1 unit", "one unit"],
        "2 Units": ["2 units", "two units"],
        "3 and 4 Units": ["3 and 4 units", "3-4 units", "3 & 4 units"],
        "5 Units or More": ["5 units or more", "5 or more units", "5+ units"],
        "Num of Structures With 5 Units or More": [
            "num of structures with 5 units or more",
            "number of structures with 5 units or more",
        ],
    }

    rename = {}
    for target, aliases in exact_candidates.items():
        for alias in aliases:
            if alias in lower_to_real:
                rename[lower_to_real[alias]] = target
                break

    csa_col  = find_col(["csa"])
    cbsa_col = find_col(["cbsa"])
    name_col = find_col(["name"]) or find_col(["area"]) or find_col(["metropolitan"])
    total_col = find_col(["total"])
    one_col   = find_col(["1", "unit"])
    two_col   = find_col(["2", "unit"])
    three4_col = find_col(["3", "4", "unit"]) or find_col(["3 and 4"]) or find_col(["3-4"])
    fivep_col  = find_col(["5", "more"]) or find_col(["5", "or more"]) or find_col(["5", "unit"])
    num_struct5_col = (
        find_col(["num", "structure", "5"])
        or find_col(["structures", "5"])
        or find_col(["num", "structures"])
    )

    if csa_col and "CSA" not in rename.values(): rename[csa_col] = "CSA"
    if cbsa_col and "CBSA" not in rename.values(): rename[cbsa_col] = "CBSA"
    if name_col and "Name" not in rename.values(): rename[name_col] = "Name"
    if total_col and "Total" not in rename.values(): rename[total_col] = "Total"
    if one_col and "1 Unit" not in rename.values(): rename[one_col] = "1 Unit"
    if two_col and "2 Units" not in rename.values(): rename[two_col] = "2 Units"
    if three4_col and "3 and 4 Units" not in rename.values(): rename[three4_col] = "3 and 4 Units"
    if fivep_col and "5 Units or More" not in rename.values(): rename[fivep_col] = "5 Units or More"
    if num_struct5_col and "Num of Structures With 5 Units or More" not in rename.values():
        rename[num_struct5_col] = "Num of Structures With 5 Units or More"

    df = df.rename(columns=rename)

    # crea faltantes
    base_cols = [c for c in EXPECTED_COLS if c != "Year"]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[base_cols]

    # limpiezas
    for c in ["CSA", "CBSA", "Name"]:
        df[c] = df[c].astype("string").str.strip()

    for c in base_cols:
        if c not in ["CSA", "CBSA", "Name"]:
            df[c] = df[c].astype("string").str.replace(",", "", regex=False).str.strip()

    return df


def pick_units_sheet(sheet_names):
    """
    Elige la hoja correcta:
    - Prioriza hojas que contengan "units"
    - Evita "value"
    """
    candidates = [s for s in sheet_names if ("unit" in s.lower() and "value" not in s.lower())]
    if candidates:
        return candidates[0]
    candidates = [s for s in sheet_names if ("unit" in s.lower())]
    if candidates:
        return candidates[0]
    return None


def load_excel_units(path: str, year: int) -> pd.DataFrame:
    """Lee la hoja de 'Units' del Excel, normaliza y añade Year."""
    xls = pd.ExcelFile(path)
    sheet = pick_units_sheet(xls.sheet_names)
    if not sheet:
        raise ValueError(f"No encuentro hoja de 'Units' en {path}. Hojas: {xls.sheet_names}")

    # Muchos archivos traen títulos/notas arriba. Detectamos la fila real de encabezado.
    raw = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)
    header_row = None
    for i in range(min(len(raw), 40)):
        cells = [str(x).strip().lower() for x in raw.iloc[i].tolist() if pd.notna(x)]
        line = " | ".join(cells)
        has_cbsa = "cbsa" in line
        has_units = "unit" in line
        has_total = "total" in line
        if has_cbsa and has_units and has_total:
            header_row = i
            break

    if header_row is None:
        # fallback al comportamiento original
        df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    else:
        headers = [str(x).strip() if pd.notna(x) else f"col_{j}" for j, x in enumerate(raw.iloc[header_row].tolist())]
        df = raw.iloc[header_row + 1 :].copy()
        df.columns = headers

    df = normalize_columns(df)
    df = df.dropna(how="all")

    # Descarta filas vacías/artefactos antes de filtrar
    df["CBSA"] = df["CBSA"].astype("string").str.strip()
    df = df[df["CBSA"].notna() & (df["CBSA"] != "")].copy()

    # filtro suave CBSA (si aplica)
    cbsa_clean = df["CBSA"]
    mask_cbsa = cbsa_clean.str.match(r"^\d{5}$", na=False)
    if mask_cbsa.sum() > 0:
        df = df[mask_cbsa].copy()

    df.insert(0, "Year", year)
    return df


def parse_tb3u_txt_robust(text: str) -> pd.DataFrame:
    """
    Parser robusto para tb3uYYYY.txt (cabecera rota).
    Detecta líneas por CBSA (5 dígitos) y extrae 6 columnas numéricas al final.
    """
    lines = text.splitlines()
    rows = []

    pat = re.compile(r"^\s*(?:(\d{3,5})\s+)?(\d{5})\s+(.*\S)\s*$")

    for line in lines:
        m = pat.match(line)
        if not m:
            continue

        csa, cbsa, rest = m.group(1), m.group(2), m.group(3)
        csa = csa if csa is not None else ""

        parts = rest.split()
        if len(parts) < 7:
            continue

        nums = parts[-6:]
        name = " ".join(parts[:-6]).strip()

        if not all(re.fullmatch(r"[-\d,]+", x) for x in nums):
            continue

        rows.append([csa, cbsa, name] + nums)

    df = pd.DataFrame(
        rows,
        columns=[
            "CSA",
            "CBSA",
            "Name",
            "Total",
            "1 Unit",
            "2 Units",
            "3 and 4 Units",
            "5 Units or More",
            "Num of Structures With 5 Units or More",
        ],
    )

    df = normalize_columns(df)

    cbsa_clean = df["CBSA"].astype("string").str.strip()
    mask_cbsa = cbsa_clean.str.match(r"^\d{5}$", na=False)
    if mask_cbsa.sum() > 0:
        df = df[mask_cbsa].copy()

    return df


def load_txt_year(year: int, url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = parse_tb3u_txt_robust(r.text)
    df.insert(0, "Year", year)
    return df


# =========================
# MAIN (2012–2025)
# =========================
all_parts = []

# TXT 2012–2018 (en orden)
for y in range(2012, 2019):
    df_y = load_txt_year(y, TXT_URLS[y])
    all_parts.append(df_y)
    print(f" TXT {y}: filas={len(df_y)} | CBSA únicos={df_y['CBSA'].nunique()}")

# Excel 2019–2025 
for stem in EXCEL_FILES:
    year = year_from_stem(stem)
    p = find_existing_excel_path(stem)
    df_x = load_excel_units(p, year)
    all_parts.append(df_x)
    print(f" Excel {stem} ({year}): filas={len(df_x)} | CBSA únicos={df_x['CBSA'].nunique()}")

# Concatena respetando el orden
out = pd.concat(all_parts, ignore_index=True)
out = out[EXPECTED_COLS]

# Export: 
out_path = os.path.join(BASE_DIR, "permits_cbsa_2012_2025.csv")
out.to_csv(out_path, index=False, encoding="utf-8")

print("\n==============================")
print("OUTPUT final creado:")
print(out_path)
print("Filas totales:", len(out), "| Columnas:", out.shape[1])
print("Columnas:", list(out.columns))
print("Primeras 5 filas:")
print(out.head())
print("==============================")
