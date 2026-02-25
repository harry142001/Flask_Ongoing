from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3, json, re
from typing import List, Dict, Any, Tuple
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "data", "Database1.db")

DB_PATH = os.getenv("DB_PATH", DEFAULT_DB_PATH)


TABLE = "properties"

app = Flask(__name__)
CORS(app)

# ---------- Helpers ----------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con
with connect() as con:
    cols = {row["name"].lower() for row in con.execute(f"PRAGMA table_info({TABLE})")}
HAS_STATE = "state" in cols
HAS_PROVINCE = "province" in cols

REGION_SQL = (
    "COALESCE(province, state)" if HAS_PROVINCE and HAS_STATE
    else ("province" if HAS_PROVINCE else "state")
)

def rows_to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]

def clean_postal(s: str) -> str:
    return (s or "").upper().replace(" ", "")

def parse_int(v, default=None):
    try:
        if v is None: return default
        return int(v)
    except Exception:
        return default

def _full_address_from_row(r: Dict[str, Any]) -> str:
    prov = r.get("province") or r.get("state")
    pc   = r.get("postcode") or r.get("postal")
    parts = [r.get("address"), r.get("city"), prov, pc]
    parts = [str(p) for p in parts if p and str(p).strip()]
    return ", ".join(parts)

def to_api_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map DB columns to API names without changing DB schema."""
    out = dict(row)
    if "state" in out:
        out["province"] = out.pop("state")
    if "postal" in out:
        out["postcode"] = out.pop("postal")
    return out

def respond(payload: List[Dict[str, Any]], view: str = "json"):
    
    view = (view or "json").lower()

    if view == "list":
        # {"Address, City, PROV, POSTCODE": "lat,lon"}
        out = {}
        for r in payload:
            lat, lon = r.get("latitude"), r.get("longitude")
            if lat is not None and lon is not None:
                out[_full_address_from_row(r)] = f"{lat},{lon}"
        return Response(json.dumps(out, indent=2), status=200, mimetype="application/json")


    # default: list of objects with count
    return Response(
        json.dumps({"count": len(payload), "items": payload}, indent=2),
        status=200,
        mimetype="application/json"
    )

# ---------- Filters ----------
def add_filters(sql: str, params: List[Any], args) -> Tuple[str, List[Any]]:
    """
    Apply WHERE clauses based on request args.
    New param names:
      - postcode  (was 'postal')
      - province  (was 'state')
    """
    q = args.get("q")
    if q:
        q_stripped = q.strip()

        # --- 1. Split into quoted phrases and free text ---
        quoted_phrases = [m.group(1).strip() for m in re.finditer(r'"(.*?)"', q_stripped)]
        free_text = re.sub(r'"(.*?)"', " ", q_stripped).strip()

        # --- 2. SUBSTRING groups for quoted phrases (NOT exact equals) ---
        for phrase in quoted_phrases:
            if not phrase:
                continue
            like = f"%{phrase}%"
            sql += (
                " AND ("
                " address LIKE ? COLLATE NOCASE"
                " OR city LIKE ? COLLATE NOCASE"
                f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                " OR agent LIKE ? COLLATE NOCASE"
                " OR broker LIKE ? COLLATE NOCASE"
                " OR CAST(latitude AS TEXT) LIKE ?"
                " OR CAST(longitude AS TEXT) LIKE ?"
                " OR REPLACE(postal,' ','') LIKE REPLACE(?,' ','')"
                ")"
            )
            params += [like, like, like, like, like, like, like, like]

        # --- 3. KEYWORD groups for leftover tokens (current behavior) ---
        if free_text:
            terms = [t.strip() for t in re.split(r"[,\s()]+", free_text) if t.strip()]
            for t in terms:
                like = f"%{t}%"
                token_clean = clean_postal(t)

                # Detect numeric-looking token for lat/lon prefix search
                is_numberish = bool(re.fullmatch(r"-?\d+(\.\d+)?", t))
                latlon_like = (t.strip() + "%") if is_numberish else like

                # Postal patterns
                is_fsa = bool(re.fullmatch(r"[A-Z]\d[A-Z]", token_clean))
                is_full_postal = bool(re.fullmatch(r"[A-Z]\d[A-Z]\d[A-Z]\d", token_clean))
                is_us_zip5 = bool(re.fullmatch(r"\d{5}", t))
                is_us_zip9 = bool(re.fullmatch(r"\d{5}-\d{4}", t))

                if is_fsa or is_full_postal:
                    sql += (
                        " AND ("
                        " address LIKE ? COLLATE NOCASE"
                        " OR city LIKE ? COLLATE NOCASE"
                        f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                        " OR agent LIKE ? COLLATE NOCASE"
                        " OR broker LIKE ? COLLATE NOCASE"
                        " OR CAST(latitude AS TEXT) LIKE ?"
                        " OR CAST(longitude AS TEXT) LIKE ?"
                        " OR REPLACE(postal,' ','') LIKE ?"
                        ")"
                    )
                    params += [like, like, like, like, like, latlon_like, latlon_like, token_clean + "%"]

                elif is_us_zip5 or is_us_zip9:
                    zip_prefix = t.split("-")[0]
                    sql += (
                        " AND ("
                        " address LIKE ? COLLATE NOCASE"
                        " OR city LIKE ? COLLATE NOCASE"
                        f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                        " OR agent LIKE ? COLLATE NOCASE"
                        " OR broker LIKE ? COLLATE NOCASE"
                        " OR CAST(latitude AS TEXT) LIKE ?"
                        " OR CAST(longitude AS TEXT) LIKE ?"
                        " OR REPLACE(postal,' ','') LIKE ?"
                        ")"
                    )
                    params += [like, like, like, like, like, latlon_like, latlon_like, zip_prefix + "%"]

                else:
                    sql += (
                        " AND ("
                        " address LIKE ? COLLATE NOCASE"
                        " OR city LIKE ? COLLATE NOCASE"
                        f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                        " OR agent LIKE ? COLLATE NOCASE"
                        " OR broker LIKE ? COLLATE NOCASE"
                        " OR CAST(latitude AS TEXT) LIKE ?"
                        " OR CAST(longitude AS TEXT) LIKE ?"
                        " OR REPLACE(postal,' ','') LIKE REPLACE(?,' ','')"
                        ")"
                    )
                    params += [like, like, like, like, like, latlon_like, latlon_like, like]

    addr = args.get("address")
    if addr:
        if addr.isdigit():
            sql += " AND address LIKE ? COLLATE NOCASE"
            params.append(f"{addr} %")
        else:
            sql += " AND REPLACE(address, ' ', '') LIKE ? COLLATE NOCASE"
            params.append(f"%{addr.replace(' ', '')}%")
    
    lat = args.get("latitude")
    if lat:
        lat_str = str(lat).strip()
        sql += " AND CAST(latitude AS TEXT) LIKE ?"
        params.append(f"{lat_str}%")

    lon = args.get("longitude")
    if lon:
        lon_str = str(lon).strip()
        sql += " AND CAST(longitude AS TEXT) LIKE ?"
        params.append(f"{lon_str}%")

    postcode = args.get("postcode")
    if postcode:
        postcode = clean_postal(postcode)
        sql += " AND REPLACE(postal,' ','') LIKE ?"
        params.append(postcode + "%")

    city = args.get("city")
    if city:
        sql += " AND city LIKE ?"
        params.append(f"%{city}%")

    agent = args.get("agent")
    if agent:
        sql += " AND agent LIKE ?"
        params.append(f"%{agent}%")

    broker = args.get("broker")
    if broker:
        sql += " AND broker LIKE ?"
        params.append(f"%{broker}%")

    region = args.get("province") or args.get("state")
    if region:
        region = str(region).strip()
        sql += f" AND UPPER({REGION_SQL}) LIKE UPPER(?)"
        params.append(f"{region}%")
    
    return sql, params


# ---------- Endpoints ----------
@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.get("/api/v1/cities")
def list_cities():
    with connect() as con:
        rows = con.execute(f"""
            SELECT DISTINCT city FROM {TABLE}
            WHERE city IS NOT NULL AND TRIM(city) <> ''
            ORDER BY city ASC
        """).fetchall()
    return jsonify([r["city"] for r in rows]), 200

@app.get("/api/v1/search")
def api_search():
    args = request.args
    view  = args.get("view", "json")
    limit = parse_int(args.get("limit"))
    page  = max(1, parse_int(args.get("page"), 1))
    offset = (page - 1) * (limit or 0)

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
        r["formatted_address"] = _full_address_from_row(r)

    return respond(rows, view)


# =====================================================
# PANDAS POWERED ENDPOINTS
# =====================================================

@app.get("/api/v1/stats")
def api_stats():
    """
    Returns counts grouped by city, province, FSA, agent, broker.
    """
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
        df["fsa"] = (
            df["postal"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.replace(" ", "", regex=False)
            .str[:3]
        )
    
    if "state" in df.columns and "province" not in df.columns:
        df["province"] = df["state"]
    
    def get_counts(column):
        if column not in df.columns:
            return {}
        return (
            df[column]
            .fillna("")
            .replace("", pd.NA)
            .dropna()
            .value_counts()
            .to_dict()
        )
    
    stats = {}
    
    if by == "all":
        stats["by_city"] = get_counts("city")
        stats["by_province"] = get_counts("province")
        stats["by_fsa"] = get_counts("fsa")
        stats["by_agent"] = get_counts("agent")
        stats["by_broker"] = get_counts("broker")
    elif by in ["city", "province", "fsa", "agent", "broker"]:
        stats[f"by_{by}"] = get_counts(by)
    else:
        return jsonify({"error": f"Unknown grouping: {by}"}), 400
    
    return jsonify({
        "count": len(df),
        "stats": stats
    }), 200


@app.get("/api/v1/data-quality")
def api_data_quality():
    """
    Returns data quality report - shows missing/empty values per field.
    """
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
            "pct_filled": round((filled / total) * 100, 1) if total > 0 else 0,
            "pct_missing": round((missing / total) * 100, 1) if total > 0 else 0
        }
    
    return jsonify({
        "count": total,
        "by_field": quality
    }), 200


@app.get("/api/v1/export/csv")
def api_export_csv():
    """
    Export filtered data as CSV file download.
    """
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
    
    if "state" in df.columns:
        df = df.rename(columns={"state": "province"})
    if "postal" in df.columns:
        df = df.rename(columns={"postal": "postcode"})
    
    csv_data = df.to_csv(index=False)
    
    return Response(
        csv_data,
        status=200,
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=export.csv"
        }
    )


@app.get("/api/v1/duplicates")
def api_duplicates():
    """
    Find duplicate records with detailed analysis.
    """
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
        return jsonify({
            "total_rows": 0,
            "summary": {},
            "duplicates": []
        }), 200
    
    total_rows = len(df)
    
    df["address_clean"] = df["address"].fillna("").astype(str).str.lower().str.strip()
    df["city_clean"] = df["city"].fillna("").astype(str).str.lower().str.strip()
    df["postal_clean"] = df["postal"].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
    
    if "state" in df.columns:
        df["province_clean"] = df["state"].fillna("").astype(str).str.lower().str.strip()
    elif "province" in df.columns:
        df["province_clean"] = df["province"].fillna("").astype(str).str.lower().str.strip()
    else:
        df["province_clean"] = ""
    
    property_keys = ["address_clean", "city_clean", "province_clean", "postal_clean"]
    
    df["price_clean"] = df["price"].fillna("").astype(str).str.strip()
    df["agent_clean"] = df["agent"].fillna("").astype(str).str.lower().str.strip()
    df["broker_clean"] = df["broker"].fillna("").astype(str).str.lower().str.strip()
    df["lat_clean"] = df["latitude"].fillna("").astype(str).str.strip()
    df["lon_clean"] = df["longitude"].fillna("").astype(str).str.strip()
    
    all_keys = property_keys + ["price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    
    true_dup_mask = df.duplicated(subset=all_keys, keep='first')
    true_dup_count = int(true_dup_mask.sum())
    true_dup_groups = int(df[df.duplicated(subset=all_keys, keep=False)].groupby(all_keys).ngroups) if true_dup_mask.any() else 0
    
    prop_dup_mask = df.duplicated(subset=property_keys, keep=False)
    prop_dup_df = df[prop_dup_mask].copy()
    
    price_variants = 0
    agent_variants = 0
    broker_variants = 0
    
    if not prop_dup_df.empty:
        for _, group in prop_dup_df.groupby(property_keys):
            if len(group) > 1:
                if group["price_clean"].nunique() > 1:
                    price_variants += len(group) - 1
                if group["agent_clean"].nunique() > 1:
                    agent_variants += len(group) - 1
                if group["broker_clean"].nunique() > 1:
                    broker_variants += len(group) - 1
    
    if dup_type == "true":
        result_df = df[true_dup_mask]
    elif dup_type == "variants":
        prop_dup_extras = df.duplicated(subset=property_keys, keep='first')
        true_dup_extras = df.duplicated(subset=all_keys, keep='first')
        variant_mask = prop_dup_extras & ~true_dup_extras
        result_df = df[variant_mask]
    else:
        result_df = df[df.duplicated(subset=property_keys, keep='first')]
    
    clean_cols = ["address_clean", "city_clean", "province_clean", "postal_clean", 
                  "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    result_df = result_df.drop(columns=clean_cols, errors='ignore')
    
    result_df = result_df.sort_values("address")
    
    duplicates = result_df.to_dict(orient="records")
    duplicates = [to_api_row(r) for r in duplicates]
    
    summary = {
        "true_duplicates": {
            "count": true_dup_count,
            "groups": true_dup_groups
        },
        "variants": {
            "price_differs": price_variants,
            "agent_differs": agent_variants,
            "broker_differs": broker_variants
        },
        "percent_duplicates": round((len(result_df) / total_rows) * 100, 2) if total_rows > 0 else 0
    }
    
    return jsonify({
        "total_rows": total_rows,
        "returned": len(duplicates),
        "type": dup_type,
        "summary": summary,
        "duplicates": duplicates
    }), 200


@app.get("/api/v1/export/geojson")
def api_export_geojson():
    """
    Export filtered data as GeoJSON for maps.
    """
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
            lat = row.get("latitude")
            lon = row.get("longitude")
            
            if lat is None or lon is None:
                continue
            if str(lat).strip().upper() in ('', 'NAN', 'NONE', 'NULL'):
                continue
            if str(lon).strip().upper() in ('', 'NAN', 'NONE', 'NULL'):
                continue
                
            lat = float(lat)
            lon = float(lon)
            
            if lat != lat or lon != lon:
                continue
                
        except (TypeError, ValueError):
            continue
        
        properties = {}
        for key, value in row.items():
            if key not in ("latitude", "longitude"):
                if key == "state":
                    properties["province"] = value
                elif key == "postal":
                    properties["postcode"] = value
                else:
                    properties[key] = value
        
        properties["formatted_address"] = _full_address_from_row(row)
        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": properties
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    download = args.get("download", "").lower() == "true"
    
    if download:
        return Response(
            json.dumps(geojson, indent=2),
            status=200,
            mimetype="application/geo+json",
            headers={
                "Content-Disposition": "attachment; filename=export.geojson"
            }
        )
    else:
        return Response(
            json.dumps(geojson),
            status=200,
            mimetype="application/json"
        )


# =====================================================
# NEW ENDPOINTS
# =====================================================

@app.get("/api/v1/recent")
def api_recent():
    """
    Returns recently added properties (newest first).
    
    Usage:
        /api/v1/recent                → latest 50 properties
        /api/v1/recent?limit=100      → latest 100 properties
        /api/v1/recent?city=Toronto   → latest properties in Toronto
    """
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
        r["formatted_address"] = _full_address_from_row(r)
    
    return jsonify({
        "count": len(rows),
        "items": rows
    }), 200


@app.get("/api/v1/search/clean")
def api_search_clean():
    """
    Returns filtered addresses with TRUE DUPLICATES removed.
    Keeps only unique records - no exact copies.
    
    Usage:
        /api/v1/search/clean                    → all unique properties
        /api/v1/search/clean?city=Toronto       → unique properties in Toronto
        /api/v1/search/clean?agent=John         → unique properties by agent
        /api/v1/search/clean?limit=1000         → limit results
        /api/v1/search/clean?view=list          → summary format (address: lat,lon)
        /api/v1/search/clean?view=details       → full details (default)
    
    Note: True duplicates = exact match on address, city, province, postal,
          price, agent, broker, latitude, longitude
    """
    args = request.args
    limit = parse_int(args.get("limit"), 50000)
    view = (args.get("view") or "details").lower()
    
    sql = f"SELECT rowid AS id, * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " ORDER BY rowid DESC LIMIT ?"
    params.append(limit)
    
    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())
    
    if not rows:
        if view == "list":
            return Response(json.dumps({}, indent=2), status=200, mimetype="application/json")
        return Response(json.dumps({"count": 0, "duplicates_removed": 0, "items": []}, indent=2), status=200, mimetype="application/json")
    
    original_count = len(rows)
    df = pd.DataFrame(rows)
    
    df["address_clean"] = df["address"].fillna("").astype(str).str.lower().str.strip()
    df["city_clean"] = df["city"].fillna("").astype(str).str.lower().str.strip()
    df["postal_clean"] = df["postal"].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
    
    if "state" in df.columns:
        df["province_clean"] = df["state"].fillna("").astype(str).str.lower().str.strip()
    elif "province" in df.columns:
        df["province_clean"] = df["province"].fillna("").astype(str).str.lower().str.strip()
    else:
        df["province_clean"] = ""
    
    df["price_clean"] = df["price"].fillna("").astype(str).str.strip()
    df["agent_clean"] = df["agent"].fillna("").astype(str).str.lower().str.strip()
    df["broker_clean"] = df["broker"].fillna("").astype(str).str.lower().str.strip()
    df["lat_clean"] = df["latitude"].fillna("").astype(str).str.strip()
    df["lon_clean"] = df["longitude"].fillna("").astype(str).str.strip()
    
    all_keys = ["address_clean", "city_clean", "province_clean", "postal_clean",
                "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    
    df_clean = df.drop_duplicates(subset=all_keys, keep='first')
    
    clean_cols = ["address_clean", "city_clean", "province_clean", "postal_clean",
                  "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    df_clean = df_clean.drop(columns=clean_cols, errors='ignore')
    
    rows_clean = df_clean.to_dict(orient="records")
    rows_clean = [to_api_row(r) for r in rows_clean]
    
    for r in rows_clean:
        r["formatted_address"] = _full_address_from_row(r)
    
    duplicates_removed = original_count - len(rows_clean)
    
    # Handle view=list (summary format)
    if view == "list":
        out = {}
        for r in rows_clean:
            lat, lon = r.get("latitude"), r.get("longitude")
            if lat is not None and lon is not None:
                out[_full_address_from_row(r)] = f"{lat},{lon}"
        return Response(json.dumps(out, indent=2), status=200, mimetype="application/json")
    
    # Default: view=details (full JSON with pretty printing)
    return Response(
        json.dumps({
            "count": len(rows_clean),
            "duplicates_removed": duplicates_removed,
            "items": rows_clean
        }, indent=2),
        status=200,
        mimetype="application/json"
    )


# =====================================================
# PROPERTY DETAILS ENDPOINTS (detailed property data)
# =====================================================

def connect_details():
    """Connect to property details database"""
    details_db = os.path.join(BASE_DIR, "data", "property_details.db")
    con = sqlite3.connect(details_db)
    con.row_factory = sqlite3.Row
    return con


@app.get("/api/v1/property/details")
def api_property_details():
    """
    Query detailed property information (bedrooms, bathrooms, lot size, etc.)
    
    Usage:
        /api/v1/property/details                    → all properties
        /api/v1/property/details?address=Clark      → search by address
        /api/v1/property/details?city=Toronto       → filter by city
        /api/v1/property/details?pin=065020114      → search by PIN
        /api/v1/property/details?bedrooms=3         → filter by bedrooms
        /api/v1/property/details?min_value=500000   → min assessed value
        /api/v1/property/details?max_value=1000000  → max assessed value
    """
    args = request.args
    limit = parse_int(args.get("limit"), 100)
    
    sql = "SELECT * FROM property_details WHERE 1=1"
    params = []
    
    # Address search
    address = args.get("address")
    if address:
        sql += " AND address LIKE ? COLLATE NOCASE"
        params.append(f"%{address}%")
    
    # City filter
    city = args.get("city")
    if city:
        sql += " AND city LIKE ? COLLATE NOCASE"
        params.append(f"%{city}%")
    
    # PIN search
    pin = args.get("pin")
    if pin:
        sql += " AND pin = ?"
        params.append(pin)
    
    # Bedrooms filter
    bedrooms = args.get("bedrooms")
    if bedrooms:
        sql += " AND bedrooms = ?"
        params.append(int(bedrooms))
    
    # Min bedrooms
    min_beds = args.get("min_bedrooms")
    if min_beds:
        sql += " AND bedrooms >= ?"
        params.append(int(min_beds))
    
    # Bathrooms filter
    bathrooms = args.get("bathrooms")
    if bathrooms:
        sql += " AND full_bathrooms = ?"
        params.append(int(bathrooms))
    
    # Assessed value range
    min_value = args.get("min_value")
    if min_value:
        sql += " AND assessed_value >= ?"
        params.append(float(min_value))
    
    max_value = args.get("max_value")
    if max_value:
        sql += " AND assessed_value <= ?"
        params.append(float(max_value))
    
    # Year built
    year_built = args.get("year_built")
    if year_built:
        sql += " AND year_built = ?"
        params.append(int(year_built))
    
    # Has pool
    has_pool = args.get("has_pool")
    if has_pool and has_pool.lower() == "true":
        sql += " AND (indoor_pool = 'Y' OR outdoor_pool = 'Y')"
    
    # Has garage
    has_garage = args.get("has_garage")
    if has_garage and has_garage.lower() == "true":
        sql += " AND garage_spaces > 0"
    
    # Zoning
    zoning = args.get("zoning")
    if zoning:
        sql += " AND zoning = ?"
        params.append(zoning)
    
    sql += " LIMIT ?"
    params.append(limit)
    
    try:
        with connect_details() as con:
            rows = [dict(r) for r in con.execute(sql, params).fetchall()]
        
        # Parse sales_history JSON string back to list
        for row in rows:
            if row.get('sales_history'):
                try:
                    row['sales_history'] = json.loads(row['sales_history'])
                except:
                    pass
        
        return Response(
            json.dumps({"count": len(rows), "items": rows}, indent=2),
            status=200,
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)