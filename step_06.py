#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 17:42:02 2026

@author: vegagonzalez
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 15:39:39 2026

@author: vegagonzalez
"""

# -*- coding: utf-8 -*-
"""
PANEL MASTER (incremental) — SIN counties

STEP 1:
- Base panel (CBSACode + MSACode + State/Metro/CBSAName)
- year/month (desde unemployment)
- unemployment_value (BLS LA), mapeando series_id -> CBSACode via lma-directory
- Mantiene SOLO filas con MSACode no vacío y unemployment_value no vacío

STEP 2:
- Añade real_personal_income desde:
  /Users/vegagonzalez/Desktop/rents/RPP/MARPP_MSA_2008_2023.csv
- Cruce:
  panel.CBSACode == MARPP.GeoFIPS
  panel.year == year (melt horizontal -> vertical)
- Filtra a TableName = MARPP y LineCode = 1
- Solo años >= 2012

STEP 3:
- Añade population desde:
  /Users/vegagonzalez/Desktop/rents/population_2012_2024.csv
- Cruce:
  population col B (CBSA/GEO_ID) -> panel CBSACode
  population col D -> nueva columna "population"
- Si el archivo trae Year (normal), se cruza también por year.
  (Si no trae Year, se cruza solo por CBSACode)

STEP 4:
- Añade Aland_sqm desde:
  /Users/vegagonzalez/Desktop/rents/2024_Gaz_cbsa_national.txt
- Cruce:
  gazetteer.GEOID -> panel.CBSACode
- Añadir a la derecha: Aland_sqm (desde columna ALAND del gazetteer)
- SOLO mantener filas con match (Aland_sqm no vacío)

STEP 5:
- Añade Permits desde:
  /Users/vegagonzalez/Desktop/rents/permits_cbsa_2012_2025.csv
- Cruce:
  permits col B (según tu instrucción) -> panel.CBSACode
  y (si existe Year) también por year
- Añadir a la derecha: Permits (desde columna E del fichero, renombrada)

STEP 6:
- Añade Zori_index desde:
  /Users/vegagonzalez/Desktop/rents/Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv
- Convierte ZORI de formato wide a long: RegionID, year, month, Zori_index
- Cruce:
  panel.MSACode == zori.RegionID
  panel.year == zori.year
  panel.month == zori.month
- Ignora el día (solo year y month)

STEP 7:
- Añade dos columnas de HOAM desde:
  /Users/vegagonzalez/Desktop/rents/HOAM_CBSA_Data.xlsx
- Cruce:
  panel.CBSACode == HOAM columna B
- Añade al final del panel las columnas D y E del Excel con sus nombres originales.
  Si vienen sin encabezado usable (por ejemplo "Unnamed: ..."), usa HOAM_D y HOAM_E.
- Merge LEFT many-to-one (sin perder filas del panel). Si HOAM trae duplicados por CBSACode,
  colapsa tomando el primer valor no nulo de D/E por código.

OUTPUT:
- /Users/vegagonzalez/Desktop/rents/panel_master_step7_cbsa_unemployment_rpi_population_aland_permits_zori_hoam.csv
"""

import os
import re
import pandas as pd

BASE_DIR = "/Users/vegagonzalez/Desktop/rents"

# Inputs STEP 1
LIST_CBSA_XLSX = os.path.join(BASE_DIR, "list1_2023_metropolitan.xlsx")
CROSSWALK_XLSX = os.path.join(BASE_DIR, "CountyCrossWalk_Zillow.xlsx")
UNEMP_CSV = os.path.join(BASE_DIR, "la.data.60.Metro_ENDS03.csv")
LMA_DIR_XLSX = os.path.join(BASE_DIR, "lma-directory-2025.xlsx")

# Inputs STEP 2-5
RPI_CSV = os.path.join(BASE_DIR, "RPP", "MARPP_MSA_2008_2023.csv")
POP_CSV = os.path.join(BASE_DIR, "population_2012_2024.csv")
GAZ_TXT = os.path.join(BASE_DIR, "2024_Gaz_cbsa_national.txt")
PERMITS_CSV = os.path.join(BASE_DIR, "permits_cbsa_2012_2025.csv")
ZORI_CSV = os.path.join(BASE_DIR, "Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv")
HOAM_XLSX = os.path.join(BASE_DIR, "HOAM_CBSA_Data.xlsx")

# Output
OUT_PATH = os.path.join(
    BASE_DIR,
    "panel_master_step7_cbsa_unemployment_rpi_population_aland_permits_zori_hoam.csv"
)


# -------------------------
# Helpers
# -------------------------
def zfill_5(x):
    """Convierte codigos tipo 123.0 / 123 / '00123' -> '00123' (string). Extrae 5 dígitos si viene en formato GEO."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    s = s.replace('"', "").replace("'", "")
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\s+", "", s)
    if s.isdigit():
        return s.zfill(5)
    m = re.search(r"(\d{5})$", s)
    if m:
        return m.group(1)
    return pd.NA


def strip_columns(df):
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()
    return df


def standardize_series_id(s):
    """
    Estandariza series_id para que coincida aunque hayas recortado antes:
    - Si empieza por 'LA' + letra (ej. 'LAS...' o 'LAU...'), quita las 3 primeras letras.
    - Si termina en '03', quita los 2 ultimos caracteres.
    """
    if pd.isna(s):
        return pd.NA
    x = str(s).strip()
    if len(x) >= 3 and x.startswith("LA") and x[2].isalpha():
        x = x[3:]
    if x.endswith("03"):
        x = x[:-2]
    return x.strip()


def cbsa_from_geo_like(val):
    """Alias: extrae CBSACode de strings tipo '310M500US10180'."""
    return zfill_5(val)


def read_text_table_guess_sep(path: str) -> pd.DataFrame:
    """
    Lee un .txt tipo gazetteer probando separadores comunes (tab, pipe, coma).
    Devuelve df con columnas ya strip().
    """
    for sep in ["\t", "|", ","]:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, engine="python")
            df = strip_columns(df)
            if df.shape[1] > 1:
                return df
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, engine="python", encoding="latin1")
                df = strip_columns(df)
                if df.shape[1] > 1:
                    return df
            except Exception:
                pass
        except Exception:
            pass
    raise ValueError("No pude leer el TXT con separadores tab/pipe/coma: " + path)


def clean_header_name(header_value, fallback_name: str) -> str:
    """Limpia encabezados vacíos/Unnamed; si no hay nombre usable retorna fallback."""
    if pd.isna(header_value):
        return fallback_name
    h = str(header_value).strip()
    if h == "" or h.lower().startswith("unnamed:"):
        return fallback_name
    return h


def first_non_null(series: pd.Series):
    """Devuelve el primer valor no nulo/no vacío de una serie; si no existe, NA."""
    valid = series.dropna().astype("string").str.strip()
    valid = valid[valid != ""]
    if len(valid) == 0:
        return pd.NA
    return valid.iloc[0]


# ============================================================
# STEP 1A: Base panel (CBSACode list + Crosswalk) SIN county
# ============================================================
print("\n==============================")
print("STEP 1A: Build base panel from CBSACode list + CountyCrossWalk (no counties)")
print("==============================")

lst = pd.read_excel(LIST_CBSA_XLSX, dtype=str)
cbsa_codes = lst.iloc[:, 0].astype("string").str.strip().map(zfill_5)

cbsa_codes = (
    cbsa_codes[cbsa_codes.str.fullmatch(r"\d{5}", na=False)]
    .drop_duplicates()
    .sort_values()
)
cbsa_df = pd.DataFrame({"CBSACode": cbsa_codes}).reset_index(drop=True)
print("CBSACodes en list1 (validos):", len(cbsa_df))

# Crosswalk: B=StateName, E=MetroName, F=CBSAName, H=MSACode, J=CBSACode
cw_raw = pd.read_excel(CROSSWALK_XLSX, dtype=str)
cw = cw_raw.iloc[:, [1, 4, 5, 7, 9]].copy()
cw.columns = ["StateName", "MetroName", "CBSAName", "MSACode", "CBSACode"]

for c in cw.columns:
    cw[c] = cw[c].astype("string").str.strip()

cw["CBSACode"] = cw["CBSACode"].map(zfill_5)

base = cbsa_df.merge(cw, on="CBSACode", how="left")

# Filtro: solo filas con MSACode no vacio
base["MSACode"] = base["MSACode"].astype("string").str.strip()
base = base[base["MSACode"].notna() & (base["MSACode"] != "")].copy()

# 1 fila por CBSACode (si hubiese duplicados)
base = (
    base.sort_values(["CBSACode", "StateName", "MetroName"], na_position="last")
        .groupby("CBSACode", as_index=False)
        .agg({
            "MSACode": "first",
            "StateName": "first",
            "MetroName": "first",
            "CBSAName": "first",
        })
)

print("CBSACodes unicos en base:", base["CBSACode"].nunique())
print("Filas base (CBSA-level):", len(base))


# ============================================================
# STEP 1B: Unemployment (series_id -> CBSACode) + time index
# ============================================================
print("\n==============================")
print("STEP 1B: Prepare unemployment and map to CBSACode")
print("==============================")

u = pd.read_csv(UNEMP_CSV, dtype=str)
u = strip_columns(u)

required_cols = {"series_id", "year", "period", "value"}
if not required_cols.issubset(set(u.columns)):
    raise ValueError(
        "El CSV de unemployment no tiene las columnas esperadas. Columnas detectadas: "
        + str(list(u.columns))
    )

u["series_id"] = u["series_id"].astype("string").str.strip()
u["series_id_key"] = u["series_id"].map(standardize_series_id)

u["year"] = u["year"].astype("string").str.strip()
u["period"] = u["period"].astype("string").str.strip()

# Solo meses M01..M12
u = u[u["period"].str.match(r"^M(0[1-9]|1[0-2])$", na=False)].copy()
u["month"] = u["period"].str.replace("M", "", regex=False).str.zfill(2)

u["unemployment_value"] = u["value"].astype("string").str.strip()
u = u[["series_id_key", "year", "month", "unemployment_value"]].copy()

# Limita a 2012 en adelante
u["year_int"] = pd.to_numeric(u["year"], errors="coerce")
u = u[u["year_int"] >= 2012].copy()
u = u.drop(columns=["year_int"])

lma_raw = pd.read_excel(LMA_DIR_XLSX, dtype=str)
lma = lma_raw.iloc[:, [1, 2]].copy()
lma.columns = ["series_id", "CBSACode"]
lma["series_id"] = lma["series_id"].astype("string").str.strip()
lma["series_id_key"] = lma["series_id"].map(standardize_series_id)
lma["CBSACode"] = lma["CBSACode"].map(zfill_5)

u2 = u.merge(lma[["series_id_key", "CBSACode"]], on="series_id_key", how="left")
missing_map = int(u2["CBSACode"].isna().sum())
print("Unemployment rows sin mapeo a CBSACode:", missing_map)

u2 = u2.dropna(subset=["CBSACode"]).copy()

u2 = (
    u2.groupby(["CBSACode", "year", "month"], as_index=False)["unemployment_value"]
    .first()
)

# Filtra base a CBSACodes que SI tienen unemployment (tu planteamiento original)
base_before = len(base)
base = base[base["CBSACode"].isin(set(u2["CBSACode"]))].copy()
print("Base filtrada a CBSACodes con unemployment:", base_before, "->", len(base))

time_index = (
    u2[["year", "month"]]
    .drop_duplicates()
    .sort_values(["year", "month"])
    .reset_index(drop=True)
)

# ============================================================
# STEP 1C: Panel = base x time + unemployment (y filtrar unemployment no vacio)
# ============================================================
print("\n==============================")
print("STEP 1C: Build panel and merge unemployment")
print("==============================")

base["_k"] = 1
time_index["_k"] = 1
panel = base.merge(time_index, on="_k", how="outer").drop(columns=["_k"])
base = base.drop(columns=["_k"])
time_index = time_index.drop(columns=["_k"])

panel = panel.merge(u2, on=["CBSACode", "year", "month"], how="left")

# Filtro: unemployment_value no vacio (tu planteamiento original)
panel["unemployment_value"] = panel["unemployment_value"].astype("string").str.strip()
panel = panel[panel["unemployment_value"].notna() & (panel["unemployment_value"] != "")].copy()

panel = panel[
    [
        "CBSACode",
        "MSACode",
        "StateName",
        "MetroName",
        "CBSAName",
        "year",
        "month",
        "unemployment_value",
    ]
].copy()

print("Panel STEP 1 rows:", len(panel), "| cols:", panel.shape[1])


# ============================================================
# STEP 2: Add real_personal_income (MARPP) by CBSACode + year
# ============================================================
print("\n==============================")
print("STEP 2: Add real_personal_income from MARPP_MSA_2008_2023.csv")
print("==============================")

# A veces viene tab-delimited aunque sea .csv
rpi_wide = pd.read_csv(RPI_CSV, dtype=str)
rpi_wide = strip_columns(rpi_wide)
if "GeoFIPS" not in rpi_wide.columns and len(rpi_wide.columns) == 1:
    rpi_wide = pd.read_csv(RPI_CSV, dtype=str, sep="\t")
    rpi_wide = strip_columns(rpi_wide)

for need in ["GeoFIPS", "TableName", "LineCode"]:
    if need not in rpi_wide.columns:
        raise ValueError(
            "No encuentro columna '" + need + "' en MARPP_MSA_2008_2023.csv. Columnas: "
            + str(list(rpi_wide.columns))
        )

rpi_wide["GeoFIPS"] = rpi_wide["GeoFIPS"].astype("string").str.strip().str.replace('"', "", regex=False)
rpi_wide["GeoFIPS"] = rpi_wide["GeoFIPS"].map(zfill_5)

# Filtrar serie correcta
rpi_filtered = rpi_wide[
    (rpi_wide["TableName"].astype("string").str.strip() == "MARPP")
    & (rpi_wide["LineCode"].astype("string").str.strip().isin(["1", "1.0"]))
].copy()

year_cols = [c for c in rpi_filtered.columns if re.fullmatch(r"(19|20)\d{2}", str(c).strip())]
if len(year_cols) == 0:
    raise ValueError("No pude detectar columnas de año (2008..2023) en MARPP_MSA_2008_2023.csv.")

rpi_long = rpi_filtered.melt(
    id_vars=["GeoFIPS"],
    value_vars=year_cols,
    var_name="year",
    value_name="real_personal_income",
)

rpi_long["year"] = rpi_long["year"].astype("string").str.strip()
rpi_long["real_personal_income"] = rpi_long["real_personal_income"].astype("string").str.strip()

rpi_long["year_int"] = pd.to_numeric(rpi_long["year"], errors="coerce")
rpi_long = rpi_long[rpi_long["year_int"] >= 2012].copy()
rpi_long = rpi_long.drop(columns=["year_int"])

panel2 = panel.merge(
    rpi_long,
    left_on=["CBSACode", "year"],
    right_on=["GeoFIPS", "year"],
    how="left",
).drop(columns=["GeoFIPS"])

print("Celdas real_personal_income NO vacias:", int(panel2["real_personal_income"].notna().sum()), "de", len(panel2))


# ============================================================
# STEP 3: Add population from population_2012_2024.csv
# ============================================================
print("\n==============================")
print("STEP 3: Add population from population_2012_2024.csv")
print("==============================")

pop_raw = pd.read_csv(POP_CSV, dtype=str)
pop_raw = strip_columns(pop_raw)

if pop_raw.shape[1] < 4:
    raise ValueError("population_2012_2024.csv tiene menos de 4 columnas. Columnas: " + str(list(pop_raw.columns)))

pop = pop_raw.iloc[:, [0, 1, 3]].copy()
pop.columns = ["colA_raw", "colB_raw", "population"]

colA_clean = pop["colA_raw"].astype("string").str.strip()
looks_like_year = colA_clean.str.fullmatch(r"(19|20)\d{2}", na=False).mean() > 0.5

pop["CBSACode"] = pop["colB_raw"].map(cbsa_from_geo_like)
pop["population"] = pop["population"].astype("string").str.replace(",", "", regex=False).str.strip()

if looks_like_year:
    pop["year"] = colA_clean
    pop = pop.dropna(subset=["CBSACode", "year"]).copy()
    pop = pop.groupby(["CBSACode", "year"], as_index=False)["population"].first()
    panel3 = panel2.merge(pop, on=["CBSACode", "year"], how="left")
else:
    pop = pop.dropna(subset=["CBSACode"]).copy()
    pop = pop.groupby(["CBSACode"], as_index=False)["population"].first()
    panel3 = panel2.merge(pop, on=["CBSACode"], how="left")

print("Celdas population NO vacias:", int(panel3["population"].notna().sum()), "de", len(panel3))


# ============================================================
# STEP 4: Add Aland_sqm from 2024_Gaz_cbsa_national.txt (GEOID -> CBSACode)
# ============================================================
print("\n==============================")
print("STEP 4: Add Aland_sqm from 2024_Gaz_cbsa_national.txt")
print("==============================")

gaz = read_text_table_guess_sep(GAZ_TXT)

if "GEOID" not in gaz.columns:
    raise ValueError("No encuentro columna 'GEOID' en 2024_Gaz_cbsa_national.txt. Columnas: " + str(list(gaz.columns)))
if "ALAND" not in gaz.columns:
    raise ValueError("No encuentro columna 'ALAND' en 2024_Gaz_cbsa_national.txt. Columnas: " + str(list(gaz.columns)))

gaz2 = gaz[["GEOID", "ALAND"]].copy()
gaz2["GEOID"] = gaz2["GEOID"].map(zfill_5)
gaz2["Aland_sqm"] = gaz2["ALAND"].astype("string").str.replace(",", "", regex=False).str.strip()
gaz2 = gaz2.drop(columns=["ALAND"]).drop_duplicates(subset=["GEOID"])

panel4 = panel3.merge(gaz2, left_on="CBSACode", right_on="GEOID", how="left").drop(columns=["GEOID"])

# SOLO mantener cruces con ALAND (tu instrucción)
panel4["Aland_sqm"] = panel4["Aland_sqm"].astype("string").str.strip()
panel4 = panel4[panel4["Aland_sqm"].notna() & (panel4["Aland_sqm"] != "")].copy()

print("Filas tras filtrar Aland_sqm no vacío:", len(panel4))


# ============================================================
# STEP 5: Add Permits from permits_cbsa_2012_2025.csv (annual -> replicate by month)
# ============================================================
print("\n==============================")
print("STEP 5: Add Permits from permits_cbsa_2012_2025.csv")
print("==============================")

perm_raw = pd.read_csv(PERMITS_CSV, dtype=str)
perm_raw = strip_columns(perm_raw)

if perm_raw.shape[1] < 5:
    raise ValueError("permits_cbsa_2012_2025.csv tiene menos de 5 columnas. Columnas: " + str(list(perm_raw.columns)))

# Según tu instrucción:
# - Columna B (idx 1) se relaciona con CBSACode (lo intentamos primero).
# - Columna E (idx 4) es el valor que llamaremos "Permits".
# Si el archivo trae Year (normal), lo usamos para cruzar también por year.
perm = perm_raw.iloc[:, [0, 1, 4]].copy()
perm.columns = ["colA_raw", "colB_raw", "Permits"]

# Detectar si colA es year (típico)
colA_clean = perm["colA_raw"].astype("string").str.strip()
looks_like_year = colA_clean.str.fullmatch(r"(19|20)\d{2}", na=False).mean() > 0.5

# CBSACode desde col B
perm["CBSACode_from_B"] = perm["colB_raw"].map(cbsa_from_geo_like)

# Como fallback (por si realmente el CBSA está en col C en tu archivo),
# probamos también con la col C si existe y elegimos la que más "matchea".
best_code = perm["CBSACode_from_B"]
best_match = best_code.isin(set(panel4["CBSACode"])).mean()

if perm_raw.shape[1] >= 3:
    colC_raw = perm_raw.iloc[:, 2]
    candC = colC_raw.map(cbsa_from_geo_like)
    matchC = candC.isin(set(panel4["CBSACode"])).mean()
    if matchC > best_match:
        best_code = candC
        best_match = matchC

perm["CBSACode"] = best_code

# Limpia Permits
perm["Permits"] = perm["Permits"].astype("string").str.replace(",", "", regex=False).str.strip()

if looks_like_year:
    perm["year"] = colA_clean
    perm = perm.dropna(subset=["CBSACode", "year"]).copy()
    perm = perm.groupby(["CBSACode", "year"], as_index=False)["Permits"].first()
    panel5 = panel4.merge(perm, on=["CBSACode", "year"], how="left")
else:
    perm = perm.dropna(subset=["CBSACode"]).copy()
    perm = perm.groupby(["CBSACode"], as_index=False)["Permits"].first()
    panel5 = panel4.merge(perm, on=["CBSACode"], how="left")

print("Celdas Permits NO vacias:", int(panel5["Permits"].notna().sum()), "de", len(panel5))


# ============================================================
# STEP 6: Add Zori_index from Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv
# ============================================================
print("\n==============================")
print("STEP 6: Add Zori_index from Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv")
print("==============================")

zori_raw = pd.read_csv(ZORI_CSV, dtype=str)
zori_raw = strip_columns(zori_raw)

if "RegionID" not in zori_raw.columns:
    raise ValueError(
        "No encuentro columna 'RegionID' en Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv. Columnas: "
        + str(list(zori_raw.columns))
    )

date_cols = [
    c for c in zori_raw.columns
    if re.fullmatch(r"(19|20)\d{2}-\d{2}-\d{2}", str(c).strip())
]
if len(date_cols) == 0:
    raise ValueError(
        "No pude detectar columnas de fecha YYYY-MM-DD en Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv."
    )

zori_long = zori_raw.melt(
    id_vars=["RegionID"],
    value_vars=date_cols,
    var_name="date",
    value_name="Zori_index",
)

zori_long["date"] = pd.to_datetime(zori_long["date"], errors="coerce")
zori_long = zori_long.dropna(subset=["date"]).copy()

zori_long["RegionID"] = zori_long["RegionID"].astype("string").str.strip()
zori_long["year"] = zori_long["date"].dt.year.astype(str)
zori_long["month"] = zori_long["date"].dt.month.astype(str).str.zfill(2)
zori_long["Zori_index"] = zori_long["Zori_index"].astype("string").str.strip()

zori_long = (
    zori_long[["RegionID", "year", "month", "Zori_index"]]
    .groupby(["RegionID", "year", "month"], as_index=False)["Zori_index"]
    .first()
)

panel5["MSACode"] = panel5["MSACode"].astype("string").str.strip()
panel6 = panel5.merge(
    zori_long,
    left_on=["MSACode", "year", "month"],
    right_on=["RegionID", "year", "month"],
    how="inner",
).drop(columns=["RegionID"])

print("Filas tras drop de no-cruce MSACode/RegionID/year/month:", len(panel6))
print("Celdas Zori_index NO vacias:", int(panel6["Zori_index"].notna().sum()), "de", len(panel6))

# QC: validar faltantes de Zori_index solo esperados en 2012-2014
panel6["Zori_index"] = panel6["Zori_index"].astype("string").str.strip()
panel6["year_int"] = pd.to_numeric(panel6["year"], errors="coerce")
missing_zori = panel6[(panel6["year_int"] >= 2015) & (panel6["Zori_index"].isna() | (panel6["Zori_index"] == ""))].copy()
if len(missing_zori) == 0:
    print("QC ZORI: OK - No hay blancos en Zori_index para años >= 2015")
else:
    print("QC ZORI: WARNING - Hay", len(missing_zori), "filas con Zori_index en blanco para años >= 2015")
    print("QC ZORI: Blancos por año (>=2015):")
    print(missing_zori.groupby("year", as_index=False).size().head(20))

# Orden final: Zori_index a la derecha
panel6 = panel6[
    [
        "CBSACode",
        "MSACode",
        "StateName",
        "MetroName",
        "CBSAName",
        "year",
        "month",
        "unemployment_value",
        "real_personal_income",
        "population",
        "Aland_sqm",
        "Permits",
        "Zori_index",
    ]
].copy()


# ============================================================
# STEP 7: Add HOAM columns D/E from HOAM_CBSA_Data.xlsx (col B -> CBSACode)
# ============================================================
print("\n==============================")
print("STEP 7: Add HOAM columns D/E from HOAM_CBSA_Data.xlsx")
print("==============================")

# Limpieza IN-PLACE de identificador en panel (sin columnas auxiliares)
panel6["CBSACode"] = panel6["CBSACode"].map(zfill_5)

hoam_raw = pd.read_excel(HOAM_XLSX, dtype=str)
hoam_raw = strip_columns(hoam_raw)

if hoam_raw.shape[1] < 5:
    raise ValueError(
        "HOAM_CBSA_Data.xlsx tiene menos de 5 columnas; necesito B, D y E. Columnas: "
        + str(list(hoam_raw.columns))
    )

hoam_key_name = hoam_raw.columns[1]
hoam_d_name = clean_header_name(hoam_raw.columns[3], "HOAM_D")
hoam_e_name = clean_header_name(hoam_raw.columns[4], "HOAM_E")

# Evitar colisiones de nombre (si D y E traen el mismo header)
if hoam_e_name == hoam_d_name:
    hoam_e_name = f"{hoam_e_name}_E"

hoam = hoam_raw.iloc[:, [1, 3, 4]].copy()
hoam.columns = [hoam_key_name, hoam_d_name, hoam_e_name]

# Estandarización de clave para cruce many-to-one
hoam[hoam_key_name] = hoam[hoam_key_name].map(zfill_5)
hoam = hoam.dropna(subset=[hoam_key_name]).copy()

# Colapso de duplicados por CBSACode conservando primer valor no nulo para D y E
hoam = (
    hoam.groupby(hoam_key_name, as_index=False)
    .agg({
        hoam_d_name: first_non_null,
        hoam_e_name: first_non_null,
    })
)

# LEFT JOIN sobre panel, agregando nuevas columnas al final
panel7 = panel6.merge(
    hoam,
    left_on="CBSACode",
    right_on=hoam_key_name,
    how="left",
    validate="many_to_one",
)

if hoam_key_name != "CBSACode":
    panel7 = panel7.drop(columns=[hoam_key_name])

print("Celdas", hoam_d_name, "NO vacias:", int(panel7[hoam_d_name].notna().sum()), "de", len(panel7))
print("Celdas", hoam_e_name, "NO vacias:", int(panel7[hoam_e_name].notna().sum()), "de", len(panel7))

panel7.to_csv(OUT_PATH, index=False, encoding="utf-8")

print("\n==============================")
print("OK - Output creado:", OUT_PATH)
print("Filas:", len(panel7), "| Columnas:", panel7.shape[1])
print("Ejemplo filas:")
print(panel7.head(5))
print("==============================")
