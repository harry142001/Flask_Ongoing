from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3, json, re
from typing import List, Dict, Any, Tuple

DB_PATH = "Database1.db"
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
    """Build a readable address string using API field names when available."""
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
    """
    Standardize output for frontend:
      - view=json  (default): { count, items: [objects] }
      - view=list            : { "Address, City, Province, Postcode": "lat,lon", ... }
      - view=geojson         : GeoJSON FeatureCollection
    """
    view = (view or "json").lower()

    if view == "list":
        # {"Address, City, PROV, POSTCODE": "lat,lon"}
        out = {}
        for r in payload:
            lat, lon = r.get("latitude"), r.get("longitude")
            if lat is not None and lon is not None:
                out[_full_address_from_row(r)] = f"{lat},{lon}"
        return Response(json.dumps(out, indent=2), status=200, mimetype="application/json")

    if view == "geojson":
        fc = {"type": "FeatureCollection", "features": []}
        for r in payload:
            lat, lon = r.get("latitude"), r.get("longitude")
            if lat is None or lon is None:
                continue
            # keep all other fields in properties (already normalized)
            props = {k: v for k, v in r.items() if k not in ("latitude", "longitude")}
            fc["features"].append({
                "type": "Feature",
                "properties": props,
                "geometry": {"type": "Point", "coordinates": [lon, lat]}
            })
        return Response(json.dumps(fc, indent=2), status=200, mimetype="application/json")

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
    # Free-text across address/city/postal (DB column is still 'postal')
       # ----- One-box "formatted address" search (street/city/province|state/postal) -----
    # ----- One-box "formatted address" search (street/city/province|state/postal) -----
    q = args.get("q")
    if q:
        # Split on spaces/commas/parentheses/hyphens; keep non-empty tokens
        terms = [t.strip() for t in re.split(r"[,\s()\-]+", q) if t.strip()]
        for t in terms:
            like = f"%{t}%"

            # Normalize token for postal checks (upper, no spaces)
            token_clean = clean_postal(t)

            # Detect Canadian FSA (A1A) or full postal (A1A1A1), and US ZIP (12345 or 12345-6789)
            is_fsa = bool(re.fullmatch(r"[A-Z]\d[A-Z]", token_clean))
            is_full_postal = bool(re.fullmatch(r"[A-Z]\d[A-Z]\d[A-Z]\d", token_clean))
            is_us_zip5 = bool(re.fullmatch(r"\d{5}", t))
            is_us_zip9 = bool(re.fullmatch(r"\d{5}-\d{4}", t))

            if is_fsa or is_full_postal:
                # For Canadian postals, anchor to START of postal (ignore spaces)
                sql += (
                    " AND ("
                    " address LIKE ? COLLATE NOCASE"
                    " OR city LIKE ? COLLATE NOCASE"
                    f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                    " OR REPLACE(postal,' ','') LIKE ?"  # prefix match, no spaces
                    ")"
                )
                params += [like, like, like, token_clean + "%"]

            elif is_us_zip5 or is_us_zip9:
                # For US ZIP, anchor to START; allow exact 5 or ZIP+4
                # Normalize DB (no spaces) then do prefix on '12345' or exact '12345-6789'
                zip_prefix = t.split("-")[0]  # '12345'
                sql += (
                    " AND ("
                    " address LIKE ? COLLATE NOCASE"
                    " OR city LIKE ? COLLATE NOCASE"
                    f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                    " OR REPLACE(postal,' ','') LIKE ?"   # e.g., '12345%'
                    ")"
                )
                params += [like, like, like, zip_prefix + "%"]

            else:
                # General substring matching for all other tokens
                sql += (
                    " AND ("
                    " address LIKE ? COLLATE NOCASE"
                    " OR city LIKE ? COLLATE NOCASE"
                    f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                    " OR REPLACE(postal,' ','') LIKE REPLACE(?,' ','')"  # substring
                    ")"
                )
                params += [like, like, like, like]



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
    """
    One flexible search endpoint for the frontend form.

    Optional query params (new names):
      q, city, postcode, agent, broker, province,
      limit (default 50), page (default 1),
      view=json|list|geojson

    Notes:
      - DB still uses columns 'postal' and 'state'.
      - Output is normalized to 'postcode' and 'province'.
    """
    args = request.args
    view  = args.get("view", "json")
    limit = parse_int(args.get("limit"))
    page  = max(1, parse_int(args.get("page"), 1))
    offset = (page - 1) * (limit or 0)

    sql = f"SELECT rowid AS id, * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, args)

    # If you do not want ordering, comment out the next line.
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


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port, debug=True)
