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
    parts = [p for p in parts if p and str(p).strip()]
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
    # treat postal leniently: ignore spaces and allow substring
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
        # If only digits: match house number at the beginning
        if addr.isdigit():
            sql += " AND address LIKE ? COLLATE NOCASE"
            params.append(f"{addr} %")
        else:
            # Flexible match: ignore spaces in both DB and input
            sql += " AND REPLACE(address, ' ', '') LIKE ? COLLATE NOCASE"
            params.append(f"%{addr.replace(' ', '')}%")
    
    lat = args.get("latitude")
    if lat:
        lat_str = str(lat).strip()
    # prefix match: works for both partial and full values
        sql += " AND CAST(latitude AS TEXT) LIKE ?"
        params.append(f"{lat_str}%")

# Longitude prefix match (accepts lon or longitude)
    lon = args.get("longitude")
    if lon:
        lon_str = str(lon).strip()
        sql += " AND CAST(longitude AS TEXT) LIKE ?"
        params.append(f"{lon_str}%")


    # Postcode prefix/full (DB column is 'postal')
    postcode = args.get("postcode")
    if postcode:
        postcode = clean_postal(postcode)
        sql += " AND REPLACE(postal,' ','') LIKE ?"
        params.append(postcode + "%")

    # City substring
    city = args.get("city")
    if city:
        sql += " AND city LIKE ?"
        params.append(f"%{city}%")

    # Agent / Broker substring
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

    # Normalize keys in the response (state->province, postal->postcode)
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
    Good for bar charts and pie charts.
    
    Usage:
        /api/v1/stats                     → all stats
        /api/v1/stats?by=city             → only city counts
        /api/v1/stats?by=agent            → only agent counts
        /api/v1/stats?city=Toronto        → stats filtered by city
    
    Supports all the same filters as /search (q, city, province, agent, etc.)
    """
    args = request.args
    
    # Which grouping to return (or "all" for everything)
    by = args.get("by", "all").lower()
    limit = parse_int(args.get("limit"), 50000)
    
    # Build filtered query
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
    
    # Add FSA column
    if "postal" in df.columns:
        df["fsa"] = (
            df["postal"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.replace(" ", "", regex=False)
            .str[:3]
        )
    
    # Normalize province/state
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
    
    Usage:
        /api/v1/data-quality              → quality report for all data
        /api/v1/data-quality?city=Toronto → quality report filtered by city
    
    Supports all the same filters as /search
    """
    args = request.args
    limit = parse_int(args.get("limit"), 50000)
    
    # Build filtered query
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
    
    # Calculate missing/empty for each column
    quality = {}
    for col in df.columns:
        # Count filled values (non-null and non-empty)
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
    
    Usage:
        /api/v1/export/csv                → export all data
        /api/v1/export/csv?city=Toronto   → export filtered data
        /api/v1/export/csv?limit=1000     → limit rows
    
    Supports all the same filters as /search
    """
    args = request.args
    limit = parse_int(args.get("limit"), 50000)
    
    # Build filtered query
    sql = f"SELECT * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)
    
    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())
    
    df = pd.DataFrame(rows)
    
    # Rename columns for API consistency
    if "state" in df.columns:
        df = df.rename(columns={"state": "province"})
    if "postal" in df.columns:
        df = df.rename(columns={"postal": "postcode"})
    
    # Convert to CSV
    csv_data = df.to_csv(index=False)
    
    # Return as downloadable file
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
    
    Usage:
        /api/v1/duplicates                    → all duplicates with summary
        /api/v1/duplicates?type=true          → complete duplicates (everything matches)
        /api/v1/duplicates?type=variants      → same property, different price/agent/broker
        /api/v1/duplicates?type=all           → both (default)
        /api/v1/duplicates?city=Toronto       → filter by city
    
    Supports all the same filters as /search
    """
    args = request.args
    dup_type = args.get("type", "all").lower()  # true, variants, all
    limit = parse_int(args.get("limit"), 50000)
    
    # Build filtered query
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
    
    # Normalize columns for comparison
    df["address_clean"] = df["address"].fillna("").astype(str).str.lower().str.strip()
    df["city_clean"] = df["city"].fillna("").astype(str).str.lower().str.strip()
    df["postal_clean"] = df["postal"].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
    
    # Handle state/province
    if "state" in df.columns:
        df["province_clean"] = df["state"].fillna("").astype(str).str.lower().str.strip()
    elif "province" in df.columns:
        df["province_clean"] = df["province"].fillna("").astype(str).str.lower().str.strip()
    else:
        df["province_clean"] = ""
    
    # Property key = address + city + province + postal (identifies unique property)
    property_keys = ["address_clean", "city_clean", "province_clean", "postal_clean"]
    
    # All columns for true duplicate check (including lat/lon)
    df["price_clean"] = df["price"].fillna("").astype(str).str.strip()
    df["agent_clean"] = df["agent"].fillna("").astype(str).str.lower().str.strip()
    df["broker_clean"] = df["broker"].fillna("").astype(str).str.lower().str.strip()
    df["lat_clean"] = df["latitude"].fillna("").astype(str).str.strip()
    df["lon_clean"] = df["longitude"].fillna("").astype(str).str.strip()
    
    all_keys = property_keys + ["price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    
    # --- Calculate summary stats ---
    
    # True duplicates: same property + same price + same agent + same broker
    true_dup_mask = df.duplicated(subset=all_keys, keep='first')
    true_dup_count = int(true_dup_mask.sum())
    true_dup_groups = int(df[df.duplicated(subset=all_keys, keep=False)].groupby(all_keys).ngroups) if true_dup_mask.any() else 0
    
    # Property duplicates: same address+city+province+postal (may have different price/agent/broker)
    prop_dup_mask = df.duplicated(subset=property_keys, keep=False)
    prop_dup_df = df[prop_dup_mask].copy()
    
    # Variants: same property but something differs
    price_variants = 0
    agent_variants = 0
    broker_variants = 0
    
    if not prop_dup_df.empty:
        # Group by property and check for variations
        for _, group in prop_dup_df.groupby(property_keys):
            if len(group) > 1:
                if group["price_clean"].nunique() > 1:
                    price_variants += len(group) - 1
                if group["agent_clean"].nunique() > 1:
                    agent_variants += len(group) - 1
                if group["broker_clean"].nunique() > 1:
                    broker_variants += len(group) - 1
    
    # --- Get duplicate records based on type ---
    
    if dup_type == "true":
        # Only complete duplicates
        result_df = df[true_dup_mask]
    elif dup_type == "variants":
        # Same property but price/agent/broker differs
        # First get all property duplicates, then exclude true duplicates
        prop_dup_extras = df.duplicated(subset=property_keys, keep='first')
        true_dup_extras = df.duplicated(subset=all_keys, keep='first')
        variant_mask = prop_dup_extras & ~true_dup_extras
        result_df = df[variant_mask]
    else:
        # All: any property that appears more than once (extras only)
        result_df = df[df.duplicated(subset=property_keys, keep='first')]
    
    # Clean up helper columns
    clean_cols = ["address_clean", "city_clean", "province_clean", "postal_clean", 
                  "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean"]
    result_df = result_df.drop(columns=clean_cols, errors='ignore')
    
    # Sort by address
    result_df = result_df.sort_values("address")
    
    # Convert to list
    duplicates = result_df.to_dict(orient="records")
    duplicates = [to_api_row(r) for r in duplicates]
    
    # Build summary
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
    Export filtered data as GeoJSON for maps (Leaflet, Mapbox, etc.)
    
    Usage:
        /api/v1/export/geojson                → export all data with coordinates
        /api/v1/export/geojson?city=Toronto   → export filtered data
        /api/v1/export/geojson?limit=1000     → limit features
    
    Supports all the same filters as /search
    Only includes records that have both latitude and longitude.
    """
    args = request.args
    limit = parse_int(args.get("limit"), 50000)
    
    # Build filtered query - only get records with coordinates
    sql = f"SELECT * FROM {TABLE} WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)
    sql += " LIMIT ?"
    params.append(limit)
    
    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())
    
    # Convert to GeoJSON
    features = []
    for row in rows:
        try:
            lat = float(row.get("latitude"))
            lon = float(row.get("longitude"))
        except (TypeError, ValueError):
            continue  # Skip if lat/lon can't be converted
        
        # Build properties (all fields except lat/lon)
        properties = {}
        for key, value in row.items():
            if key not in ("latitude", "longitude"):
                # Rename for API consistency
                if key == "state":
                    properties["province"] = value
                elif key == "postal":
                    properties["postcode"] = value
                else:
                    properties[key] = value
        
        # Add formatted address
        properties["formatted_address"] = _full_address_from_row(row)
        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]  # GeoJSON uses [longitude, latitude]
            },
            "properties": properties
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return Response(
        json.dumps(geojson, indent=2),
        status=200,
        mimetype="application/geo+json",
        headers={
            "Content-Disposition": "attachment; filename=export.geojson"
        }
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)