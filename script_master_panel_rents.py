#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 02:23:36 2026

@author: vegagonzalez
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PANEL MASTER — panel_master_rents.csv

CAMBIOS (últimos):
- El panel empieza en 2016-05 (incluido).
- Dropear CBSACodes: 45860, 21460, 36020, 38240 (45860 repetido en tu lista; se elimina una sola vez).
- Mantener ventana final hasta 2023.
- Renombrar Permits -> Building_permits.
- Código con notas por STEP.

OUTPUT:
- /Users/vegagonzalez/Desktop/rents/panel_master_rents_clean.csv
"""

import os
import re
import pandas as pd

# ============================================================
# RUTAS (misma carpeta base)
# ============================================================
BASE_DIR = "/Users/vegagonzalez/Desktop/rents"

LIST_CBSA_XLSX = os.path.join(BASE_DIR, "list1_2023_metropolitan.xlsx")
CROSSWALK_XLSX = os.path.join(BASE_DIR, "CountyCrossWalk_Zillow.xlsx")

UNEMP_CSV = os.path.join(BASE_DIR, "la.data.60.Metro_ENDS03.csv")
LMA_DIR_XLSX = os.path.join(BASE_DIR, "lma-directory-2025.xlsx")

RPI_CSV = os.path.join(BASE_DIR, "RPP", "MARPP_MSA_2008_2023.csv")
POP_CSV = os.path.join(BASE_DIR, "population_2012_2024.csv")
GAZ_TXT = os.path.join(BASE_DIR, "2024_Gaz_cbsa_national.txt")
PERMITS_CSV = os.path.join(BASE_DIR, "permits_cbsa_2012_2025.csv")

ZORI_CSV = os.path.join(BASE_DIR, "Metro_zori_uc_sfrcondomfr_sm_sa_month_cleaned.csv")
HOAM_XLSX = os.path.join(BASE_DIR, "HOAM_CBSA_Data.xlsx")
AFF_CSV = os.path.join(
    BASE_DIR,
    "Metro_new_homeowner_income_needed_downpayment_0.20_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)

# Macros (Steps 9–13)
MPS_XLSX = os.path.join(BASE_DIR, "monetary-policy-surprises-data.xlsx")
EFFR_CSV = os.path.join(BASE_DIR, "EFFR.csv")
FEDFUNDS_CSV = os.path.join(BASE_DIR, "FEDFUNDS.csv")
MORTGAGE30US_CSV = os.path.join(BASE_DIR, "MORTGAGE30US.csv")
PCEPILFE_CSV = os.path.join(BASE_DIR, "PCEPILFE.csv")

# OUTPUT final pedido
OUT_PATH = os.path.join(BASE_DIR, "panel_master_rents_clean.csv")

# ============================================================
# VENTANAS Y FILTROS PEDIDOS
# ============================================================
# (i) Ventana máxima por año
PANEL_YEAR_MIN = 2015
PANEL_YEAR_MAX = 2023

# (ii) Inicio exacto del panel (inclusive)
PANEL_START_YEAR = 2016
PANEL_START_MONTH = 5  # mayo

# (iii) CBSAs a dropear (col A del panel = CBSACode)
DROP_CBSAS_RAW = ["45860", "21460", "36020", "38240", "45860"]  # viene repetido 45860


# ============================================================
# HELPERS (limpieza / parsing / agregación)
# ============================================================
def strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()
    return df


def zfill_5(x):
    """CBSACode helper: devuelve 5 dígitos si posible."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().replace('"', "").replace("'", "")
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\s+", "", s)
    if s.isdigit():
        return s.zfill(5)
    m = re.search(r"(\d{5})$", s)
    return m.group(1) if m else pd.NA


def only_digits(x):
    """Normaliza IDs tipo float/string y devuelve solo dígitos (o NA si queda vacío)."""
    if pd.isna(x):
        return pd.NA
    s = str(x).replace('"', "").replace("'", "").strip()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"\D", "", s)
    return s if s else pd.NA


def standardize_series_id(s):
    """Standardiza series_id para join con lma-directory aunque haya recortes previos."""
    if pd.isna(s):
        return pd.NA
    x = str(s).strip()
    if len(x) >= 3 and x.startswith("LA") and x[2].isalpha():
        x = x[3:]
    if x.endswith("03"):
        x = x[:-2]
    return x.strip()


def cbsa_from_geo_like(val):
    return zfill_5(val)


def read_text_table_guess_sep(path: str) -> pd.DataFrame:
    """Lee un .txt tipo gazetteer probando separadores comunes."""
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


def first_non_null(series: pd.Series):
    """Devuelve el primer valor no nulo (y no vacío si es string)."""
    s = series.dropna()
    if len(s) == 0:
        return pd.NA
    if s.dtype == "object":
        s2 = s.astype(str).str.strip()
        s2 = s2[s2 != ""]
        return s2.iloc[0] if len(s2) else pd.NA
    return s.iloc[0]


def load_daily_to_monthly_first_value(csv_path: str, value_name: str,
                                      start_year: int = 2015, end_year: int = 2023) -> pd.DataFrame:
    """
    CSV con headers:
      - observation_date en formato USA M/D/YY
      - columna value_name
    Devuelve mensual (year, month, value_name) tomando el PRIMER día disponible del mes.
    Restringe a [start_year, end_year].
    """
    df = pd.read_csv(csv_path, dtype=str)
    df = strip_columns(df)

    cols_lower = {c: str(c).strip().lower() for c in df.columns}

    # fecha
    date_col = None
    for c, cl in cols_lower.items():
        if cl in {"observation_date", "observation", "date", "observationdate"}:
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]

    # valor
    val_col = None
    for c, cl in cols_lower.items():
        if cl == value_name.lower():
            val_col = c
            break
    if val_col is None:
        val_col = df.columns[1] if len(df.columns) >= 2 else None
    if val_col is None:
        raise ValueError(f"{os.path.basename(csv_path)} no tiene columna de valores detectable.")

    tmp = df[[date_col, val_col]].copy()
    tmp.columns = ["date_raw", value_name]

    s = tmp["date_raw"].astype("string").str.strip()

    # FORMATO correcto: M/D/YY
    dt = pd.to_datetime(s, format="%m/%d/%y", errors="coerce")
    dt2 = pd.to_datetime(s, errors="coerce", dayfirst=False)
    dt = dt.fillna(dt2)

    tmp["date"] = dt
    tmp = tmp.dropna(subset=["date"]).copy()

    tmp["year"] = tmp["date"].dt.year
    tmp["month"] = tmp["date"].dt.month

    tmp[value_name] = pd.to_numeric(
        tmp[value_name].astype("string").str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )

    tmp = tmp.dropna(subset=[value_name]).copy()
    tmp = tmp[(tmp["year"] >= start_year) & (tmp["year"] <= end_year)].copy()
    tmp = tmp.sort_values(["year", "month", "date"])

    monthly = (
        tmp.groupby(["year", "month"], as_index=False)
           .first()[["year", "month", value_name]]
    )

    monthly["year"] = monthly["year"].astype("Int64").astype("string")
    monthly["month"] = monthly["month"].astype("Int64").astype("string").str.zfill(2)
    return monthly


def load_mps_orth(path_xlsx: str) -> pd.DataFrame:
    """Carga monetary-policy-surprises-data.xlsx y devuelve (year, month, MPS_ORTH) normalizado."""
    try:
        sheets = pd.read_excel(path_xlsx, sheet_name=None, dtype=str, engine="openpyxl")
    except Exception:
        sheets = {"__default__": pd.read_excel(path_xlsx, dtype=str, engine="openpyxl")}

    picked = None
    picked_name = None

    for sh_name, df in sheets.items():
        df = strip_columns(df)
        upper = {c: str(c).strip().upper() for c in df.columns}

        def find_col(target_upper: str):
            for c in df.columns:
                if upper[c] == target_upper:
                    return c
            return None

        y = find_col("YEAR")
        m = find_col("MONTH")
        o = find_col("MPS_ORTH")
        if y and m and o:
            picked = df[[y, m, o]].copy()
            picked.columns = ["year", "month", "MPS_ORTH"]
            picked_name = sh_name
            break

    if picked is None:
        all_cols = {sh: list(strip_columns(df).columns) for sh, df in sheets.items()}
        raise ValueError(
            "No encontré columnas Year, Month y MPS_ORTH en monetary-policy-surprises-data.xlsx.\n"
            f"Columnas detectadas por hoja: {all_cols}"
        )

    print(f"Hoja usada para MPS: {picked_name}")

    picked["year"] = pd.to_numeric(picked["year"].map(only_digits), errors="coerce")
    picked["month"] = pd.to_numeric(picked["month"].map(only_digits), errors="coerce")
    picked["MPS_ORTH"] = pd.to_numeric(picked["MPS_ORTH"], errors="coerce")

    picked = picked.dropna(subset=["year", "month"]).copy()
    picked = picked[(picked["month"] >= 1) & (picked["month"] <= 12)].copy()

    picked["year"] = picked["year"].astype("Int64").astype("string")
    picked["month"] = picked["month"].astype("Int64").astype("string").str.zfill(2)

    picked = (
        picked.groupby(["year", "month"], as_index=False)["MPS_ORTH"]
              .agg(lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA)
    )
    return picked


# ============================================================
# STEP 1 — BASE: listado CBSA + crosswalk CBSA↔MSA (1 fila por CBSACode)
# ============================================================
print("\n==============================")
print("STEP 1: Base CBSA + Crosswalk (sin counties)")
print("==============================")

lst = pd.read_excel(LIST_CBSA_XLSX, dtype=str, engine="openpyxl")
cbsa_codes = lst.iloc[:, 0].astype("string").str.strip().map(zfill_5)
cbsa_codes = cbsa_codes[cbsa_codes.str.fullmatch(r"\d{5}", na=False)].drop_duplicates().sort_values()
cbsa_df = pd.DataFrame({"CBSACode": cbsa_codes}).reset_index(drop=True)

cw_raw = pd.read_excel(CROSSWALK_XLSX, dtype=str, engine="openpyxl")
cw = cw_raw.iloc[:, [1, 4, 5, 7, 9]].copy()  # B,E,F,H,J
cw.columns = ["StateName", "MetroName", "CBSAName", "MSACode", "CBSACode"]

for c in cw.columns:
    cw[c] = cw[c].astype("string").str.strip()

cw["CBSACode"] = cw["CBSACode"].map(zfill_5)
cw["MSACode"] = cw["MSACode"].map(only_digits)

base = cbsa_df.merge(cw, on="CBSACode", how="left")
base["MSACode"] = base["MSACode"].astype("string").str.strip()
base = base[base["MSACode"].notna() & (base["MSACode"] != "")].copy()

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

print("CBSACodes base (MSACode válido):", base["CBSACode"].nunique())


# ============================================================
# STEP 2 — ZORI: fija muestra + calendario mensual (INNER) + FILTROS PEDIDOS
#   - mantener SOLO 2015–2023
#   - empezar desde 2016-05 (inclusive)
#   - dropear CBSAs indicados
# ============================================================
print("\n==============================")
print("STEP 2: ZORI fija la muestra (INNER) + filtros panel (2016-05 → 2023) + drop CBSAs")
print("==============================")

zori_raw = pd.read_csv(ZORI_CSV, dtype=str)
zori_raw = strip_columns(zori_raw)

if "RegionID" not in zori_raw.columns:
    raise ValueError("No encuentro columna 'RegionID' en ZORI. Columnas: " + str(list(zori_raw.columns)))

date_cols = [c for c in zori_raw.columns if re.fullmatch(r"(19|20)\d{2}-\d{2}-\d{2}", str(c).strip())]
if len(date_cols) == 0:
    raise ValueError("No detecté columnas fecha YYYY-MM-DD en ZORI.")

zori_long = zori_raw.melt(id_vars=["RegionID"], value_vars=date_cols, var_name="date", value_name="Zori_index")
zori_long["date"] = pd.to_datetime(zori_long["date"], errors="coerce")
zori_long = zori_long.dropna(subset=["date"]).copy()

zori_long["RegionID"] = zori_long["RegionID"].map(only_digits)
zori_long["year"] = zori_long["date"].dt.year.astype("Int64").astype("string")
zori_long["month"] = zori_long["date"].dt.month.astype("Int64").astype("string").str.zfill(2)
zori_long["Zori_index"] = pd.to_numeric(zori_long["Zori_index"], errors="coerce")

# 2015+ (en ZORI)
zori_long = zori_long[pd.to_numeric(zori_long["year"], errors="coerce") >= 2015].copy()

# 1 obs por (RegionID, year, month)
zori_long = (
    zori_long.sort_values(["RegionID", "year", "month"])
             .groupby(["RegionID", "year", "month"], as_index=False)["Zori_index"]
             .agg(first_non_null)
)

# INNER: solo MSAs y meses con ZORI
panel = base.merge(
    zori_long,
    left_on=["MSACode"],
    right_on=["RegionID"],
    how="inner"
).drop(columns=["RegionID"])

panel["year"] = panel["year"].astype("string").str.strip()
panel["month"] = panel["month"].astype("string").str.strip().str.zfill(2)

# ---- Filtro 2015–2023 (máximo) ----
year_num = pd.to_numeric(panel["year"], errors="coerce")
month_num = pd.to_numeric(panel["month"], errors="coerce")
panel = panel[year_num.between(PANEL_YEAR_MIN, PANEL_YEAR_MAX)].copy()

# ---- Filtro inicio exacto 2016-05 (inclusive) ----
year_num = pd.to_numeric(panel["year"], errors="coerce")
month_num = pd.to_numeric(panel["month"], errors="coerce")
panel = panel[
    (year_num > PANEL_START_YEAR) |
    ((year_num == PANEL_START_YEAR) & (month_num >= PANEL_START_MONTH))
].copy()

# ---- Drop CBSAs pedidos ----
drop_cbsas = sorted({zfill_5(x) for x in DROP_CBSAS_RAW if pd.notna(zfill_5(x))})
panel = panel[~panel["CBSACode"].isin(drop_cbsas)].copy()

print("CBSAs dropeados:", drop_cbsas)
print("Panel tras ZORI + filtros (filas):", len(panel))
print("CBSACodes en panel:", panel["CBSACode"].nunique())
print("Rango años-meses:", panel[["year", "month"]].min().to_dict(), "->", panel[["year", "month"]].max().to_dict())

# Guard rail: desde aquí NO debería cambiar el nº de filas por ningún step
N_AFTER_ZORI = len(panel)


# ============================================================
# STEP 3 — Unemployment (LEFT por CBSACode,year,month)
# ============================================================
print("\n==============================")
print("STEP 3: Añadir unemployment_value (LEFT)")
print("==============================")

u = pd.read_csv(UNEMP_CSV, dtype=str)
u = strip_columns(u)

needed = {"series_id", "year", "period", "value"}
if not needed.issubset(set(u.columns)):
    raise ValueError("Unemployment file missing columns. Found: " + str(list(u.columns)))

u["series_id"] = u["series_id"].astype("string").str.strip()
u["series_id_key"] = u["series_id"].map(standardize_series_id)
u["year"] = u["year"].astype("string").str.strip()
u["period"] = u["period"].astype("string").str.strip()

u = u[u["period"].str.match(r"^M(0[1-9]|1[0-2])$", na=False)].copy()
u["month"] = u["period"].str.replace("M", "", regex=False).str.zfill(2)
u["unemployment_value"] = u["value"].astype("string").str.strip()
u = u[["series_id_key", "year", "month", "unemployment_value"]].copy()

lma_raw = pd.read_excel(LMA_DIR_XLSX, dtype=str, engine="openpyxl")
lma = lma_raw.iloc[:, [1, 2]].copy()
lma.columns = ["series_id", "CBSACode"]
lma["series_id"] = lma["series_id"].astype("string").str.strip()
lma["series_id_key"] = lma["series_id"].map(standardize_series_id)
lma["CBSACode"] = lma["CBSACode"].map(zfill_5)

u2 = u.merge(lma[["series_id_key", "CBSACode"]], on="series_id_key", how="left")
u2 = u2.dropna(subset=["CBSACode"]).copy()
u2 = u2.groupby(["CBSACode", "year", "month"], as_index=False)["unemployment_value"].agg(first_non_null)

panel = panel.merge(u2, on=["CBSACode", "year", "month"], how="left")


# ============================================================
# STEP 4 — Real Personal Income (anual, LEFT por CBSACode,year)
# ============================================================
print("\n==============================")
print("STEP 4: Añadir real_personal_income (LEFT)")
print("==============================")

rpi_wide = pd.read_csv(RPI_CSV, dtype=str)
rpi_wide = strip_columns(rpi_wide)
if "GeoFIPS" not in rpi_wide.columns and len(rpi_wide.columns) == 1:
    rpi_wide = pd.read_csv(RPI_CSV, dtype=str, sep="\t")
    rpi_wide = strip_columns(rpi_wide)

for need in ["GeoFIPS", "TableName", "LineCode"]:
    if need not in rpi_wide.columns:
        raise ValueError("RPI file missing column: " + need + ". Found: " + str(list(rpi_wide.columns)))

rpi_wide["GeoFIPS"] = rpi_wide["GeoFIPS"].astype("string").str.strip().str.replace('"', "", regex=False).map(zfill_5)

rpi_filtered = rpi_wide[
    (rpi_wide["TableName"].astype("string").str.strip() == "MARPP") &
    (rpi_wide["LineCode"].astype("string").str.strip().isin(["1", "1.0"]))
].copy()

year_cols = [c for c in rpi_filtered.columns if re.fullmatch(r"(19|20)\d{2}", str(c).strip())]
if len(year_cols) == 0:
    raise ValueError("No year columns detected in MARPP file.")

rpi_long = rpi_filtered.melt(
    id_vars=["GeoFIPS"], value_vars=year_cols, var_name="year", value_name="real_personal_income"
)
rpi_long["year"] = rpi_long["year"].astype("string").str.strip()
rpi_long["real_personal_income"] = rpi_long["real_personal_income"].astype("string").str.strip()

panel = panel.merge(
    rpi_long, left_on=["CBSACode", "year"], right_on=["GeoFIPS", "year"], how="left"
).drop(columns=["GeoFIPS"])


# ============================================================
# STEP 5 — Population (anual o constante, LEFT)
# ============================================================
print("\n==============================")
print("STEP 5: Añadir population (LEFT)")
print("==============================")

pop_raw = pd.read_csv(POP_CSV, dtype=str)
pop_raw = strip_columns(pop_raw)
if pop_raw.shape[1] < 4:
    raise ValueError("population_2012_2024.csv tiene menos de 4 columnas.")

pop = pop_raw.iloc[:, [0, 1, 3]].copy()
pop.columns = ["colA_raw", "colB_raw", "population"]

colA_clean = pop["colA_raw"].astype("string").str.strip()
looks_like_year = colA_clean.str.fullmatch(r"(19|20)\d{2}", na=False).mean() > 0.5

pop["CBSACode"] = pop["colB_raw"].map(cbsa_from_geo_like)
pop["population"] = pop["population"].astype("string").str.replace(",", "", regex=False).str.strip()

if looks_like_year:
    pop["year"] = colA_clean
    pop = pop.dropna(subset=["CBSACode", "year"]).copy()
    pop = pop.groupby(["CBSACode", "year"], as_index=False)["population"].agg(first_non_null)
    panel = panel.merge(pop, on=["CBSACode", "year"], how="left")
else:
    pop = pop.dropna(subset=["CBSACode"]).copy()
    pop = pop.groupby(["CBSACode"], as_index=False)["population"].agg(first_non_null)
    panel = panel.merge(pop, on=["CBSACode"], how="left")


# ============================================================
# STEP 6 — ALAND (LEFT; NO filtra muestra)
# ============================================================
print("\n==============================")
print("STEP 6: Añadir Aland_sqm (LEFT; NO filtra muestra)")
print("==============================")

gaz = read_text_table_guess_sep(GAZ_TXT)
if "GEOID" not in gaz.columns or "ALAND" not in gaz.columns:
    raise ValueError("Gazetteer missing GEOID/ALAND columns. Found: " + str(list(gaz.columns)))

gaz2 = gaz[["GEOID", "ALAND"]].copy()
gaz2["GEOID"] = gaz2["GEOID"].map(zfill_5)
gaz2["Aland_sqm"] = gaz2["ALAND"].astype("string").str.replace(",", "", regex=False).str.strip()
gaz2 = gaz2.drop(columns=["ALAND"]).drop_duplicates(subset=["GEOID"])

panel = panel.merge(gaz2, left_on="CBSACode", right_on="GEOID", how="left").drop(columns=["GEOID"])
panel["Aland_sqm"] = panel["Aland_sqm"].astype("string").str.strip()


# ============================================================
# STEP 7 — Building permits (LEFT) + rename Permits -> Building_permits
# ============================================================
print("\n==============================")
print("STEP 7: Añadir Building_permits (LEFT)")
print("==============================")

perm_raw = pd.read_csv(PERMITS_CSV, dtype=str)
perm_raw = strip_columns(perm_raw)
if perm_raw.shape[1] < 5:
    raise ValueError("permits_cbsa_2012_2025.csv tiene menos de 5 columnas.")

perm = perm_raw.iloc[:, [0, 1, 4]].copy()
perm.columns = ["colA_raw", "colB_raw", "Permits"]

colA_clean = perm["colA_raw"].astype("string").str.strip()
looks_like_year = colA_clean.str.fullmatch(r"(19|20)\d{2}", na=False).mean() > 0.5

candB = perm["colB_raw"].map(cbsa_from_geo_like)
best_code = candB
best_match = candB.isin(set(panel["CBSACode"])).mean()

if perm_raw.shape[1] >= 3:
    candC = perm_raw.iloc[:, 2].map(cbsa_from_geo_like)
    matchC = candC.isin(set(panel["CBSACode"])).mean()
    if matchC > best_match:
        best_code = candC

perm["CBSACode"] = best_code
perm["Permits"] = perm["Permits"].astype("string").str.replace(",", "", regex=False).str.strip()

if looks_like_year:
    perm["year"] = colA_clean
    perm = perm.dropna(subset=["CBSACode", "year"]).copy()
    perm = perm.groupby(["CBSACode", "year"], as_index=False)["Permits"].agg(first_non_null)
    panel = panel.merge(perm, on=["CBSACode", "year"], how="left")
else:
    perm = perm.dropna(subset=["CBSACode"]).copy()
    perm = perm.groupby(["CBSACode"], as_index=False)["Permits"].agg(first_non_null)
    panel = panel.merge(perm, on=["CBSACode"], how="left")

panel = panel.rename(columns={"Permits": "Building_permits"})


# ============================================================
# STEP 7b — HOAM mensual (LEFT; columnas D y E)
# ============================================================
print("\n==============================")
print("STEP 7b: Añadir HOAM mensual (LEFT; columnas D y E)")
print("==============================")

hoam_raw = pd.read_excel(HOAM_XLSX, dtype=str, engine="openpyxl")
hoam_raw = strip_columns(hoam_raw)
if hoam_raw.shape[1] < 5:
    raise ValueError("HOAM_CBSA_Data.xlsx tiene menos de 5 columnas; no puedo leer B–E por posición.")

hoam = hoam_raw.iloc[:, [1, 2, 3, 4]].copy()
hoam.columns = ["CBSA_Code", "Month", "HOAM_D", "HOAM_E"]

colD_name = hoam_raw.columns[3] if isinstance(hoam_raw.columns[3], str) and hoam_raw.columns[3] != "" else "HOAM_D"
colE_name = hoam_raw.columns[4] if isinstance(hoam_raw.columns[4], str) and hoam_raw.columns[4] != "" else "HOAM_E"

hoam["CBSACode"] = hoam["CBSA_Code"].map(zfill_5)

month_str = hoam["Month"].astype("string").str.strip()
dt = pd.to_datetime(month_str, format="%Y-%m", errors="coerce")
dt2 = pd.to_datetime(month_str, errors="coerce")
dt = dt.fillna(dt2)

hoam["year"] = dt.dt.year.astype("Int64").astype("string")
hoam["month"] = dt.dt.month.astype("Int64").astype("string").str.zfill(2)
hoam = hoam[pd.to_numeric(hoam["year"], errors="coerce") >= 2015].copy()

hoam["HOAM_D"] = pd.to_numeric(hoam["HOAM_D"], errors="coerce")
hoam["HOAM_E"] = pd.to_numeric(hoam["HOAM_E"], errors="coerce")

hoam = hoam.groupby(["CBSACode", "year", "month"], as_index=False)[["HOAM_D", "HOAM_E"]].agg(
    lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA
)

panel = panel.merge(hoam, on=["CBSACode", "year", "month"], how="left")
panel = panel.rename(columns={"HOAM_D": colD_name, "HOAM_E": colE_name})


# ============================================================
# STEP 8 — Affordability_Zori (LEFT por MSACode,year,month)
# ============================================================
print("\n==============================")
print("STEP 8: Añadir Affordability_Zori (LEFT)")
print("==============================")

aff_raw = pd.read_csv(AFF_CSV, dtype=str)
aff_raw = strip_columns(aff_raw)

if "RegionID" not in aff_raw.columns:
    raise ValueError("No encuentro columna 'RegionID' en el archivo de affordability. Columnas: " + str(list(aff_raw.columns)))

aff_raw["RegionID"] = aff_raw["RegionID"].map(only_digits)
aff_date_cols = [c for c in aff_raw.columns if re.fullmatch(r"(19|20)\d{2}-\d{2}-\d{2}", str(c).strip())]

if len(aff_date_cols) > 0:
    aff_long = aff_raw.melt(id_vars=["RegionID"], value_vars=aff_date_cols, var_name="date", value_name="Affordability_Zori")
    aff_long["date"] = pd.to_datetime(aff_long["date"], errors="coerce")
    aff_long = aff_long.dropna(subset=["date"]).copy()
    aff_long["year"] = aff_long["date"].dt.year.astype("Int64").astype("string")
    aff_long["month"] = aff_long["date"].dt.month.astype("Int64").astype("string").str.zfill(2)
else:
    time_col = None
    for c in ["Month", "month", "Date", "date", "Period", "period"]:
        if c in aff_raw.columns:
            time_col = c
            break
    if time_col is None:
        raise ValueError("No veo columnas fecha (YYYY-MM-DD) ni una columna Month/Date en el archivo de affordability.")

    candidates = [c for c in aff_raw.columns if c not in ["RegionID", time_col]]
    if len(candidates) == 0:
        raise ValueError("No encuentro columna de valores en el archivo de affordability.")
    value_col = candidates[0]

    aff_long = aff_raw[["RegionID", time_col, value_col]].copy()
    aff_long = aff_long.rename(columns={time_col: "date", value_col: "Affordability_Zori"})

    dt = pd.to_datetime(aff_long["date"].astype("string").str.strip(), format="%Y-%m", errors="coerce")
    dt2 = pd.to_datetime(aff_long["date"].astype("string").str.strip(), errors="coerce")
    dt = dt.fillna(dt2)

    aff_long["year"] = dt.dt.year.astype("Int64").astype("string")
    aff_long["month"] = dt.dt.month.astype("Int64").astype("string").str.zfill(2)

aff_long["Affordability_Zori"] = pd.to_numeric(aff_long["Affordability_Zori"], errors="coerce")
aff_long = aff_long[pd.to_numeric(aff_long["year"], errors="coerce") >= 2015].copy()

aff_long = aff_long.groupby(["RegionID", "year", "month"], as_index=False)["Affordability_Zori"].agg(
    lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA
)

panel["MSACode"] = panel["MSACode"].map(only_digits)
panel["year"] = panel["year"].astype("string").str.strip()
panel["month"] = panel["month"].astype("string").str.strip().str.zfill(2)

panel = panel.merge(
    aff_long,
    left_on=["MSACode", "year", "month"],
    right_on=["RegionID", "year", "month"],
    how="left"
).drop(columns=["RegionID"])


# ============================================================
# STEP 9–13 — Macros mensuales (LEFT por year,month) (2015–2023)
#   (el panel ya está filtrado a 2016-05→2023; aquí solo añadimos columnas)
# ============================================================
print("\n==============================")
print("STEP 9: Añadir MPS_ORTH")
print("==============================")
panel = panel.merge(load_mps_orth(MPS_XLSX), on=["year", "month"], how="left")

print("\n==============================")
print("STEP 10: Añadir EFFR")
print("==============================")
panel = panel.merge(load_daily_to_monthly_first_value(EFFR_CSV, "EFFR", 2015, 2023), on=["year", "month"], how="left")

print("\n==============================")
print("STEP 11: Añadir FEDFUNDS")
print("==============================")
panel = panel.merge(load_daily_to_monthly_first_value(FEDFUNDS_CSV, "FEDFUNDS", 2015, 2023), on=["year", "month"], how="left")

print("\n==============================")
print("STEP 12: Añadir MORTGAGE30US")
print("==============================")
panel = panel.merge(load_daily_to_monthly_first_value(MORTGAGE30US_CSV, "MORTGAGE30US", 2015, 2023), on=["year", "month"], how="left")

print("\n==============================")
print("STEP 13: Añadir PCEPILFE")
print("==============================")
panel = panel.merge(load_daily_to_monthly_first_value(PCEPILFE_CSV, "PCEPILFE", 2015, 2023), on=["year", "month"], how="left")


# ============================================================
# CHECK FINAL
# ============================================================
print("\n==============================")
print("CHECK FINAL")
print("==============================")
print("Filas finales:", len(panel), "| Filas tras ZORI+filtros:", N_AFTER_ZORI)
print("Rango year-month final:", panel[["year", "month"]].min().to_dict(), "->", panel[["year", "month"]].max().to_dict())


# ============================================================
# FINAL — Orden de columnas (macro variables al final)
# ============================================================
final_cols = [
    "CBSACode",
    "MSACode",
    "StateName",
    "MetroName",
    "CBSAName",
    "year",
    "month",
    "Zori_index",
    "unemployment_value",
    "real_personal_income",
    "population",
    "Aland_sqm",
    "Building_permits",
    colD_name,
    colE_name,
    "Affordability_Zori",
    "MPS_ORTH",
    "EFFR",
    "FEDFUNDS",
    "MORTGAGE30US",
    "PCEPILFE",
]
final_cols = [c for c in final_cols if c in panel.columns]
panel = panel[final_cols].copy()

panel.to_csv(OUT_PATH, index=False, encoding="utf-8")

print("\n==============================")
print("OK - Output creado:", OUT_PATH)
print("panel_master_rents ✅")
print("Filas:", len(panel), "| Columnas:", panel.shape[1])
print("Ejemplo filas:")
print(panel.head(5))
print("==============================")