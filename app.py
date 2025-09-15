
from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import math
from typing import List, Dict, Any, Tuple

DB_PATH = "Database1.db"
TABLE = "properties"

app = Flask(__name__)
CORS(app)

# --------- Helpers ---------
def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def rows_to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]

def clean_postal(s: str) -> str:
    return (s or "").upper().replace(" ", "")

def parse_float(v, default=None):
    try:
        if v is None: return default
        return float(v)
    except Exception:
        return default

def parse_int(v, default=None):
    try:
        if v is None: return default
        return int(v)
    except Exception:
        return default

def add_filters(sql: str, params: List[Any], args) -> Tuple[str, List[Any]]:
    # Postal: match on prefix, ignore spaces
    postal = args.get("postal")
    if postal:
        postal = clean_postal(postal)
        sql += " AND REPLACE(postal, ' ', '') LIKE ?"
        params.append(postal + "%")
    # City / Agent / Broker substring matches
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
    state = args.get("state")
    if state:
        sql += " AND state = ?"
        params.append(state.upper())
    return sql, params

def add_price_sort(sql: str, args) -> str:
    # Sort by numeric price if requested
    sort = (args.get("sort") or "").lower()
    if sort in ("price_asc", "price_desc"):
        # Remove $ and , then cast to REAL (fallback 0 if null/non-numeric)
        # Note: REPLACE nested to strip symbols
        sql_price = "CAST(REPLACE(REPLACE(price, '$',''), ',', '') AS REAL)"
        direction = "ASC" if sort == "price_asc" else "DESC"
        sql += f" ORDER BY {sql_price} {direction}"
    return sql

# # Haversine distance in km
# def haversine_km(lat1, lon1, lat2, lon2):
#     R = 6371.0088
#     phi1, phi2 = math.radians(lat1), math.radians(lat2)
#     dphi = math.radians(lat2 - lat1)
#     dlambda = math.radians(lon2 - lon1)
#     a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
#     return R * c

# def bbox_km(lat, lon, radius_km):
#     # Approx bounding box (good enough for prefilter): ~111 km per degree lat
#     dlat = radius_km / 111.0
#     # adjust longitude degrees by latitude
#     dlon = radius_km / (111.320 * math.cos(math.radians(lat)) or 1e-9)
#     return (lat - dlat, lat + dlat, lon - dlon, lon + dlon)

def _full_address_from_row(r: Dict[str, Any]) -> str:

    parts = [r.get("address"), r.get("city"), r.get("state"), r.get("postal")]
    parts = [p for p in parts if p and str(p).strip()]
    return ", ".join(parts)

def _rows_to_address_map(rows: list[Dict[str, Any]]) -> Dict[str, str]:
    """Return { 'Full Address': 'lat,lon', ... } only for rows that have both coords."""
    out = {}
    for r in rows:
        lat, lon = (r.get("latitude"), r.get("longitude"))
        if lat and lon:
            out[_full_address_from_row(r)] = f"{lat},{lon}"
    return out


# --------- Endpoints ---------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/search")
def search():
    """
    Query properties with flexible filters.
    Query params:
      postal, city, agent, broker, state, sort=price_asc|price_desc
      limit, offset
      format=json | map     <-- NEW (default json)
    """
    limit = parse_int(request.args.get("limit"), 200)
    offset = parse_int(request.args.get("offset"), 0)
    limit = max(1, min(limit, 1000))  # safety

    fmt = (request.args.get("format") or "json").lower()

    sql = f"SELECT * FROM {TABLE} WHERE 1=1"
    params: List[Any] = []
    sql, params = add_filters(sql, params, request.args)
    sql = add_price_sort(sql, request.args)
    sql += " LIMIT ? OFFSET ?"
    params += [limit, offset]

    con = connect()
    try:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())
    finally:
        con.close()

    if fmt == "map":
        # Return the compact { "Full Address": "lat,lon" } dictionary
        return jsonify(_rows_to_address_map(rows))
    else:
        # Default: the regular list response
        return jsonify({"count": len(rows), "items": rows})


# @app.get("/near")
# def near():
#     """
#     Geo-radius search using Haversine (computed in Python).
#     Query params:
#       lat=43.59&lon=-79.64&radius_km=2.5
#       (Optional) postal/city/agent/broker/state filters are applied first,
#       then we prefilter via bounding box and compute accurate Haversine.
#       sort=distance (ascending)
#       limit=200 (post-filter)
#     """
#     lat = parse_float(request.args.get("lat"))
#     lon = parse_float(request.args.get("lon"))
#     radius_km = parse_float(request.args.get("radius_km"), 2.0)

#     if lat is None or lon is None:
#         return jsonify({"error": "lat and lon are required"}), 400

#     # Build base SQL with filters
#     base_sql = f"SELECT * FROM {TABLE} WHERE 1=1"
#     params: List[Any] = []
#     base_sql, params = add_filters(base_sql, params, request.args)

#     # Prefilter via bounding box
#     min_lat, max_lat, min_lon, max_lon = bbox_km(lat, lon, radius_km)
#     base_sql += " AND latitude IS NOT NULL AND longitude IS NOT NULL"
#     base_sql += " AND CAST(latitude AS REAL) BETWEEN ? AND ?"
#     base_sql += " AND CAST(longitude AS REAL) BETWEEN ? AND ?"
#     params += [min_lat, max_lat, min_lon, max_lon]

#     # Fetch candidates
#     con = connect()
#     try:
#         candidates = rows_to_dicts(con.execute(base_sql, tuple(params)).fetchall())
#     finally:
#         con.close()

#     # Compute distances
#     for r in candidates:
#         try:
#             rlat = float(r.get("latitude"))
#             rlon = float(r.get("longitude"))
#             r["distance_km"] = round(haversine_km(lat, lon, rlat, rlon), 3)
#         except Exception:
#             r["distance_km"] = None

#     # Filter within radius and sort
#     within = [r for r in candidates if r["distance_km"] is not None and r["distance_km"] <= radius_km]

#     sort = (request.args.get("sort") or "").lower()
#     if sort == "distance":
#         within.sort(key=lambda x: x["distance_km"])
#     elif sort == "price_asc":
#         def price_val(x):
#             p = (x.get("price") or "").replace("$", "").replace(",", "")
#             try: return float(p)
#             except: return float("inf")
#         within.sort(key=price_val)
#     elif sort == "price_desc":
#         def price_vald(x):
#             p = (x.get("price") or "").replace("$", "").replace(",", "")
#             try: return -float(p)
#             except: return float("-inf")
#         within.sort(key=price_vald)

#     limit = parse_int(request.args.get("limit"), 200)
#     limit = max(1, min(limit, 1000))
#     return jsonify({"count": len(within[:limit]), "items": within[:limit]})

# @app.get("/distinct")
# def distinct():
#     """
#     Quick facets for UI filters.
#     Query params: field=city|agent|broker|state
#     """
#     field = request.args.get("field")
#     if field not in {"city", "agent", "broker", "state", "postal"}:
#         return jsonify({"error": "field must be one of city, agent, broker, state, postal"}), 400

#     con = connect()
#     try:
#         rows = con.execute(f"SELECT DISTINCT {field} AS v FROM {TABLE} WHERE {field} IS NOT NULL AND TRIM({field})<>'' ORDER BY 1").fetchall()
#         vals = [r["v"] for r in rows]
#         return jsonify({"field": field, "values": vals, "count": len(vals)})
#     finally:
#         con.close()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
