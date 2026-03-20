import copy
import json
import logging
import os
import re
from typing import Any, Dict, List, Tuple

import pandas as pd
import sqlite3
from flask import Flask, Response, jsonify, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "data", "Database1.db"))
DETAILS_DB_PATH = os.path.join(BASE_DIR, "data", "property_details.db")
TABLE = "properties"

app = Flask(__name__)
CORS(app)

CACHE: Dict[str, Any] = {
    "properties": [],
    "property_details": {},
    "loaded": False,
}

# MPAC comparable field schema — values populated later
COMPARABLE_SCHEMA = {
    "property_location": "",
    "basement_finish_area": "",
    "compsaledate": "",
    "compsaleamount": "",
    "comparablesequence": "",
    "lot_depth": "",
    "lot_frontage": "",
    "municipality": "",
    "number_of_bedrooms": "",
    "number_of_full_bathrooms": "",
    "number_of_half_bathrooms": "",
    "postal_code": "",
    "property_type_style": "",
    "property_type_description": "",
    "province": "",
    "comp_roll": "",
    "lot_area": "",
    "unit_of_measure_of_lot_area": "",
    "primary_struc_area_above_grd": "",
    "year_built": "",
}




def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def connect_details():
    con = sqlite3.connect(DETAILS_DB_PATH)
    con.row_factory = sqlite3.Row
    return con


with connect() as _con:
    _cols = {r["name"].lower() for r in _con.execute(f"PRAGMA table_info({TABLE})")}

HAS_STATE = "state" in _cols
HAS_PROVINCE = "province" in _cols
REGION_SQL = (
    "COALESCE(province, state)" if HAS_PROVINCE and HAS_STATE
    else ("province" if HAS_PROVINCE else "state")
)

def load_cache():
    global CACHE
    log.info("Using DB path: %s", DB_PATH)
    log.info("BASE_DIR: %s", BASE_DIR)
    log.info("Loading data into memory...")

    try:
        with connect() as con:
            rows = con.execute(f"SELECT rowid AS id, * FROM {TABLE}").fetchall()
            CACHE["properties"] = [dict(r) for r in rows]
        log.info("Loaded %d properties", len(CACHE["properties"]))
    except Exception as e:
        log.error("Failed to load properties: %s", e)
        CACHE["properties"] = []

    try:
        main_addrs = {
            (r.get("address") or "").lower().strip()
            for r in CACHE["properties"]
        }
        with connect_details() as con:
            rows = con.execute("SELECT * FROM property_details").fetchall()
            for r in rows:
                d = dict(r)
                addr = d.get("address", "").lower().strip()
                if addr:
                    CACHE["property_details"][addr] = d
                    # If not in main DB, add as a standalone record
                    if addr not in main_addrs:
                        synthetic = {
                            "id": None,
                            "address": d.get("address", ""),
                            "city": d.get("city", ""),
                            "agent": "",
                            "broker": "",
                            "price": "",
                            "latitude": d.get("latitude", ""),
                            "longitude": d.get("longitude", ""),
                            "province": d.get("province", "") or d.get("state", ""),
                            "postcode": d.get("postal_code", "") or d.get("postcode", "") or d.get("postal", ""),
                        }
                        CACHE["properties"].append(synthetic)
        log.info("Loaded %d property details", len(CACHE["property_details"]))
    except Exception as e:
        log.error("Failed to load property details: %s", e)
        CACHE["property_details"] = {}

    CACHE["loaded"] = True
    log.info("Cache ready")


def create_indexes():
    try:
        with connect() as con:
            for col in ("city", "agent", "broker", "postal", "address", "state"):
                con.execute(f"CREATE INDEX IF NOT EXISTS idx_{col} ON {TABLE}({col})")
        log.info("DB indexes ready")
    except Exception as e:
        log.error("Index creation failed: %s", e)


def rows_to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def clean_postal(s: str) -> str:
    return (s or "").upper().replace(" ", "")


def parse_int(v, default=None):
    try:
        return int(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _full_address(r: Dict[str, Any]) -> str:
    prov = r.get("province") or r.get("state")
    pc = r.get("postcode") or r.get("postal")
    parts = [r.get("address"), r.get("city"), prov, pc]
    return ", ".join(str(p) for p in parts if p and str(p).strip())


def to_api_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    if "state" in out:
        out["province"] = out.pop("state")
    if "postal" in out:
        out["postcode"] = out.pop("postal")
    for key, value in out.items():
        if value != value:  # NaN
            out[key] = ""
        elif isinstance(value, str) and value.strip().lower() == "nan":
            out[key] = ""
    return out


def _parse_json_field(value, field_name: str):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            log.warning("Could not parse JSON for field '%s'", field_name)
            return []
    if isinstance(value, list):
        return [
            {k: ("" if v is None else v) for k, v in e.items()}
            if isinstance(e, dict) else e
            for e in value
        ]
    return []


def _clean_details(row: Dict[str, Any]) -> Dict[str, Any]:
    skip = {
        "address", "city", "province", "state", "postcode", "postal",
        "postal_code", "country", "latitude", "longitude", "price",
        "agent", "broker", "id", "notes", "comparables",  # exclude both
    }
    json_fields = {"sales_history"}
    out = {}

    for key, value in row.items():
        if key.lower() in skip:
            continue
        if key in json_fields:
            value = _parse_json_field(value, key)
        elif value is None:
            value = ""
        out[key] = value

    return out


def _attach_details(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for r in rows:
        addr = (r.get("address") or "").lower().strip()
        detail_row = CACHE["property_details"].get(addr) if addr else None
        if detail_row:
            cleaned = _clean_details(detail_row)
            r["details"] = cleaned
            # comparables at top level
            raw_comp = detail_row.get("comparables")
            comparables = _parse_json_field(raw_comp, "comparables")
            if not comparables:
                comparables = [copy.deepcopy(COMPARABLE_SCHEMA)]
            r["comparables"] = comparables
    return rows

def respond(payload: List[Dict[str, Any]], view: str = "json"):
    if view == "list":
        out = {
            _full_address(r): f"{r.get('latitude')},{r.get('longitude')}"
            for r in payload
            if r.get("latitude") is not None and r.get("longitude") is not None
        }
        return Response(json.dumps(out, indent=2), status=200, mimetype="application/json")

    return Response(
        json.dumps({"count": len(payload), "items": payload}, indent=2),
        status=200,
        mimetype="application/json",
    )


def add_filters(sql: str, params: List[Any], args) -> Tuple[str, List[Any]]:
    q = args.get("q")
    if q:
        q = q.strip()
        quoted = [m.group(1).strip() for m in re.finditer(r'"(.*?)"', q)]
        free = re.sub(r'".*?"', " ", q).strip()

        for phrase in quoted:
            if not phrase:
                continue
            like = f"%{phrase}%"
            sql += (
                " AND (address LIKE ? COLLATE NOCASE"
                " OR city LIKE ? COLLATE NOCASE"
                f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                " OR agent LIKE ? COLLATE NOCASE"
                " OR broker LIKE ? COLLATE NOCASE"
                " OR CAST(latitude AS TEXT) LIKE ?"
                " OR CAST(longitude AS TEXT) LIKE ?"
                " OR REPLACE(postal,' ','') LIKE REPLACE(?,' ',''))"
            )
            params += [like] * 8

        if free:
            for t in re.split(r"[,\s()]+", free):
                if not t:
                    continue
                like = f"%{t}%"
                token_clean = clean_postal(t)
                is_numberish = bool(re.fullmatch(r"-?\d+(\.\d+)?", t))
                latlon_like = (t + "%") if is_numberish else like
                is_fsa = bool(re.fullmatch(r"[A-Z]\d[A-Z]", token_clean))
                is_full_postal = bool(re.fullmatch(r"[A-Z]\d[A-Z]\d[A-Z]\d", token_clean))
                is_zip5 = bool(re.fullmatch(r"\d{5}", t))
                is_zip9 = bool(re.fullmatch(r"\d{5}-\d{4}", t))

                sql += (
                    " AND (address LIKE ? COLLATE NOCASE"
                    " OR city LIKE ? COLLATE NOCASE"
                    f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                    " OR agent LIKE ? COLLATE NOCASE"
                    " OR broker LIKE ? COLLATE NOCASE"
                    " OR CAST(latitude AS TEXT) LIKE ?"
                    " OR CAST(longitude AS TEXT) LIKE ?"
                    " OR REPLACE(postal,' ','') LIKE ?)"
                )
                if is_fsa or is_full_postal:
                    params += [like, like, like, like, like, latlon_like, latlon_like, token_clean + "%"]
                elif is_zip5 or is_zip9:
                    params += [like, like, like, like, like, latlon_like, latlon_like, t.split("-")[0] + "%"]
                else:
                    params += [like, like, like, like, like, latlon_like, latlon_like, like]

    addr = args.get("address")
    if addr:
        if addr.isdigit():
            sql += " AND address LIKE ? COLLATE NOCASE"
            params.append(f"{addr} %")
        else:
            sql += " AND REPLACE(address,' ','') LIKE ? COLLATE NOCASE"
            params.append(f"%{addr.replace(' ', '')}%")

    for field in ("latitude", "longitude"):
        val = args.get(field)
        if val:
            sql += f" AND CAST({field} AS TEXT) LIKE ?"
            params.append(f"{val.strip()}%")

    postcode = args.get("postcode")
    if postcode:
        sql += " AND REPLACE(postal,' ','') LIKE ?"
        params.append(clean_postal(postcode) + "%")

    city = args.get("city")
    if city:
        sql += " AND city LIKE ?"
        params.append(f"%{city}%")

    for field in ("agent", "broker"):
        val = args.get(field)
        if val:
            sql += f" AND {field} LIKE ?"
            params.append(f"%{val}%")

    region = args.get("province") or args.get("state")
    if region:
        sql += f" AND UPPER({REGION_SQL}) LIKE UPPER(?)"
        params.append(f"{region.strip()}%")

    return sql, params


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200


@app.get("/api/v1/cities")
def list_cities():
    with connect() as con:
        rows = con.execute(
            f"SELECT DISTINCT city FROM {TABLE} WHERE city IS NOT NULL AND TRIM(city) <> '' ORDER BY city"
        ).fetchall()
    return jsonify([r["city"] for r in rows]), 200


@app.get("/api/v1/search")
def api_search():
    args = request.args
    view = args.get("view", "json")
    limit = parse_int(args.get("limit"))
    page = max(1, parse_int(args.get("page"), 1))
    offset = (page - 1) * (limit or 0)
    include_details = args.get("details", "true").lower() != "false"

    filter_keys = ("q", "address", "city", "agent", "broker", "postcode", "province", "state", "latitude", "longitude")
    has_filters = any(args.get(k) for k in filter_keys)

    if not has_filters and CACHE["loaded"]:
        rows_db = CACHE["properties"].copy()
        if limit:
            rows_db = rows_db[offset:offset + limit]
    else:
        sql = f"SELECT rowid AS id, * FROM {TABLE} WHERE 1=1"
        params: List[Any] = []
        sql, params = add_filters(sql, params, args)
        sql += " ORDER BY id DESC"
        if limit:
            sql += " LIMIT ? OFFSET ?"
            params += [limit, offset]
        with connect() as con:
            rows_db = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    rows = [to_api_row(r) for r in rows_db]
    for r in rows:
        r["formatted_address"] = _full_address(r)
    rows.sort(key=lambda r: r.get("formatted_address", "").lower())

    if include_details and CACHE["loaded"]:
        rows = _attach_details(rows)

    log.info("Cache size: %d, rows_db: %d, rows after transform: %d", len(CACHE["properties"]), len(rows_db), len(rows))
    return respond(rows, view)


@app.get("/api/v1/recent")
def api_recent():
    args = request.args
    limit = parse_int(args.get("limit"), 50)
    sql = f"SELECT rowid AS id, * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " ORDER BY rowid DESC LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    rows = [to_api_row(r) for r in rows]
    for r in rows:
        r["formatted_address"] = _full_address(r)

    return jsonify({"count": len(rows), "items": rows}), 200


@app.get("/api/v1/search/clean")
def api_search_clean():
    args = request.args
    limit = parse_int(args.get("limit"), 50000)
    view = (args.get("view") or "details").lower()
    include_details = args.get("details", "true").lower() != "false"

    sql = f"SELECT rowid AS id, * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " ORDER BY rowid DESC LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    if not rows:
        empty = {} if view == "list" else {"count": 0, "duplicates_removed": 0, "items": []}
        return Response(json.dumps(empty, indent=2), status=200, mimetype="application/json")

    original_count = len(rows)
    df = pd.DataFrame(rows)

    df["address_clean"] = df["address"].fillna("").astype(str).str.lower().str.strip()
    df["city_clean"] = df["city"].fillna("").astype(str).str.lower().str.strip()
    df["postal_clean"] = df["postal"].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
    prov_col = "state" if "state" in df.columns else "province"
    df["province_clean"] = df[prov_col].fillna("").astype(str).str.lower().str.strip() if prov_col in df.columns else ""
    df["price_clean"] = df["price"].fillna("").astype(str).str.strip()
    df["agent_clean"] = df["agent"].fillna("").astype(str).str.lower().str.strip()
    df["broker_clean"] = df["broker"].fillna("").astype(str).str.lower().str.strip()
    df["lat_clean"] = df["latitude"].fillna("").astype(str).str.strip()
    df["lon_clean"] = df["longitude"].fillna("").astype(str).str.strip()

    dedup_keys = ["address_clean", "city_clean", "province_clean", "postal_clean",
                  "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]

    df = df.drop_duplicates(subset=dedup_keys, keep="first")
    df = df.drop(columns=dedup_keys, errors="ignore")

    rows_clean = [to_api_row(r) for r in df.to_dict(orient="records")]
    for r in rows_clean:
        r["formatted_address"] = _full_address(r)
    rows_clean.sort(key=lambda r: r.get("formatted_address", "").lower())

    if include_details and CACHE["loaded"]:
        rows_clean = _attach_details(rows_clean)

    if view == "list":
        out = {
            _full_address(r): f"{r.get('latitude')},{r.get('longitude')}"
            for r in rows_clean
            if r.get("latitude") is not None and r.get("longitude") is not None
        }
        return Response(json.dumps(out, indent=2), status=200, mimetype="application/json")

    return Response(
        json.dumps({
            "count": len(rows_clean),
            "duplicates_removed": original_count - len(rows_clean),
            "items": rows_clean,
        }, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.get("/api/v1/stats")
def api_stats():
    args = request.args
    by = args.get("by", "all").lower()
    limit = parse_int(args.get("limit"), 50000)

    sql = f"SELECT * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    df = pd.DataFrame(rows)
    if df.empty:
        return jsonify({"count": 0, "stats": {}}), 200

    if "postal" in df.columns:
        df["fsa"] = df["postal"].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False).str[:3]
    if "state" in df.columns and "province" not in df.columns:
        df["province"] = df["state"]

    def counts(col):
        if col not in df.columns:
            return {}
        return df[col].fillna("").replace("", pd.NA).dropna().value_counts().to_dict()

    valid_groups = {"city", "province", "fsa", "agent", "broker"}
    if by == "all":
        stats = {f"by_{g}": counts(g) for g in valid_groups}
    elif by in valid_groups:
        stats = {f"by_{by}": counts(by)}
    else:
        return jsonify({"error": f"Unknown grouping '{by}'"}), 400

    return jsonify({"count": len(df), "stats": stats}), 200


@app.get("/api/v1/data-quality")
def api_data_quality():
    args = request.args
    limit = parse_int(args.get("limit"), 50000)

    sql = f"SELECT * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    df = pd.DataFrame(rows)
    if df.empty:
        return jsonify({"count": 0, "by_field": {}}), 200

    total = len(df)
    quality = {}
    for col in df.columns:
        if df[col].dtype == object:
            filled = int((df[col].fillna("").astype(str).str.strip() != "").sum())
        else:
            filled = int(df[col].notna().sum())
        missing = total - filled
        quality[col] = {
            "total": total,
            "filled": filled,
            "missing": missing,
            "pct_filled": round(filled / total * 100, 1) if total else 0,
            "pct_missing": round(missing / total * 100, 1) if total else 0,
        }

    return jsonify({"count": total, "by_field": quality}), 200


@app.get("/api/v1/export/csv")
def api_export_csv():
    args = request.args
    limit = parse_int(args.get("limit"), 50000)

    sql = f"SELECT * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    df = pd.DataFrame(rows)
    df = df.rename(columns={"state": "province", "postal": "postcode"})

    return Response(
        df.to_csv(index=False),
        status=200,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=export.csv"},
    )


@app.get("/api/v1/export/geojson")
def api_export_geojson():
    args = request.args
    limit = parse_int(args.get("limit"), 50000)

    sql = f"SELECT * FROM {TABLE} WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    features = []
    for row in rows:
        try:
            lat, lon = row.get("latitude"), row.get("longitude")
            if str(lat).strip().upper() in ("", "NAN", "NONE", "NULL"):
                continue
            lat, lon = float(lat), float(lon)
            if lat != lat or lon != lon:
                continue
        except (TypeError, ValueError):
            continue

        props = {
            ("province" if k == "state" else "postcode" if k == "postal" else k): v
            for k, v in row.items()
            if k not in ("latitude", "longitude")
        }
        props["formatted_address"] = _full_address(row)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })

    geojson = {"type": "FeatureCollection", "features": features}
    download = args.get("download", "").lower() == "true"

    return Response(
        json.dumps(geojson, indent=2 if download else None),
        status=200,
        mimetype="application/geo+json" if download else "application/json",
        headers={"Content-Disposition": "attachment; filename=export.geojson"} if download else {},
    )


@app.get("/api/v1/duplicates")
def api_duplicates():
    args = request.args
    dup_type = args.get("type", "all").lower()
    limit = parse_int(args.get("limit"), 50000)

    sql = f"SELECT * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    df = pd.DataFrame(rows)
    if df.empty:
        return jsonify({"total_rows": 0, "summary": {}, "duplicates": []}), 200

    total_rows = len(df)
    df["address_clean"] = df["address"].fillna("").astype(str).str.lower().str.strip()
    df["city_clean"] = df["city"].fillna("").astype(str).str.lower().str.strip()
    df["postal_clean"] = df["postal"].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
    prov_col = "state" if "state" in df.columns else "province"
    df["province_clean"] = df[prov_col].fillna("").astype(str).str.lower().str.strip() if prov_col in df.columns else ""
    df["price_clean"] = df["price"].fillna("").astype(str).str.strip()
    df["agent_clean"] = df["agent"].fillna("").astype(str).str.lower().str.strip()
    df["broker_clean"] = df["broker"].fillna("").astype(str).str.lower().str.strip()
    df["lat_clean"] = df["latitude"].fillna("").astype(str).str.strip()
    df["lon_clean"] = df["longitude"].fillna("").astype(str).str.strip()

    prop_keys = ["address_clean", "city_clean", "province_clean", "postal_clean"]
    all_keys = prop_keys + ["price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]

    true_dup_mask = df.duplicated(subset=all_keys, keep="first")
    true_dup_count = int(true_dup_mask.sum())
    true_dup_groups = (
        int(df[df.duplicated(subset=all_keys, keep=False)].groupby(all_keys).ngroups)
        if true_dup_mask.any() else 0
    )

    prop_dup_df = df[df.duplicated(subset=prop_keys, keep=False)].copy()
    price_variants = agent_variants = broker_variants = 0
    for _, grp in prop_dup_df.groupby(prop_keys):
        if len(grp) > 1:
            if grp["price_clean"].nunique() > 1: price_variants += len(grp) - 1
            if grp["agent_clean"].nunique() > 1: agent_variants += len(grp) - 1
            if grp["broker_clean"].nunique() > 1: broker_variants += len(grp) - 1

    if dup_type == "true":
        result_df = df[true_dup_mask]
    elif dup_type == "variants":
        variant_mask = df.duplicated(subset=prop_keys, keep="first") & ~df.duplicated(subset=all_keys, keep="first")
        result_df = df[variant_mask]
    else:
        result_df = df[df.duplicated(subset=prop_keys, keep="first")]

    drop_cols = prop_keys + ["price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    result_df = result_df.drop(columns=drop_cols, errors="ignore").sort_values("address")
    duplicates = [to_api_row(r) for r in result_df.to_dict(orient="records")]

    return jsonify({
        "total_rows": total_rows,
        "returned": len(duplicates),
        "type": dup_type,
        "summary": {
            "true_duplicates": {"count": true_dup_count, "groups": true_dup_groups},
            "variants": {
                "price_differs": price_variants,
                "agent_differs": agent_variants,
                "broker_differs": broker_variants,
            },
            "percent_duplicates": round(len(result_df) / total_rows * 100, 2) if total_rows else 0,
        },
        "duplicates": duplicates,
    }), 200


@app.get("/api/v1/property/details")
def api_property_details():
    args = request.args
    limit = parse_int(args.get("limit"), 100)

    sql = "SELECT * FROM property_details WHERE 1=1"
    params = []

    str_filters = {
        "address": ("address LIKE ? COLLATE NOCASE", lambda v: f"%{v}%"),
        "city":    ("city LIKE ? COLLATE NOCASE",    lambda v: f"%{v}%"),
        "pin":     ("pin = ?",                        lambda v: v),
        "zoning":  ("zoning = ?",                     lambda v: v),
    }
    for key, (clause, transform) in str_filters.items():
        val = args.get(key)
        if val:
            sql += f" AND {clause}"
            params.append(transform(val))

    int_filters = {
        "bedrooms":     "bedrooms = ?",
        "min_bedrooms": "bedrooms >= ?",
        "bathrooms":    "full_bathrooms = ?",
        "year_built":   "year_built = ?",
    }
    for key, clause in int_filters.items():
        val = args.get(key)
        if val:
            if not val.isdigit():
                return jsonify({"error": f"'{key}' must be a number"}), 400
            sql += f" AND {clause}"
            params.append(int(val))

    for key, clause in [("min_value", "assessed_value >= ?"), ("max_value", "assessed_value <= ?")]:
        val = args.get(key)
        if val:
            try:
                sql += f" AND {clause}"
                params.append(float(val))
            except ValueError:
                return jsonify({"error": f"'{key}' must be a number"}), 400

    if args.get("has_pool", "").lower() == "true":
        sql += " AND (indoor_pool = 'Y' OR outdoor_pool = 'Y')"
    if args.get("has_garage", "").lower() == "true":
        sql += " AND garage_spaces > 0"

    sql += " LIMIT ?"
    params.append(limit)

    try:
        with connect_details() as con:
            rows = [dict(r) for r in con.execute(sql, params).fetchall()]

        for row in rows:
            for field in ("sales_history", "comparables"):
                row[field] = _parse_json_field(row.get(field), field)
            if not row.get("comparables"):
                row["comparables"] = [copy.deepcopy(COMPARABLE_SCHEMA)]
            if row.get("notes") is None:
                row["notes"] = ""

        return Response(
            json.dumps({"count": len(rows), "items": rows}, indent=2),
            status=200,
            mimetype="application/json",
        )
    except Exception as e:
        log.error("Property details query failed: %s", e)
        return jsonify({"error": "Internal server error"}), 500


create_indexes()
load_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)