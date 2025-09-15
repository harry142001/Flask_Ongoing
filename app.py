from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import sqlite3, math, json
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
    parts = [r.get("address"), r.get("city"), r.get("state"), r.get("postal")]
    parts = [p for p in parts if p and str(p).strip()]
    return ", ".join(parts)

def respond(payload: List[Dict[str, Any]], view: str = "json"):
    """Standardize output for frontend: json | map | geojson."""
    view = (view or "json").lower()

    if view == "map":
        # {"Address, City, ST, POSTAL": "lat,lon"}
        out = {}
        for r in payload:
            lat, lon = r.get("latitude"), r.get("longitude")
            if lat is not None and lon is not None:
                out[_full_address_from_row(r)] = f"{lat},{lon}"
        return Response(json.dumps(out, indent=2), mimetype="application/json")

    if view == "geojson":
        fc = {
            "type": "FeatureCollection",
            "features": []
        }
        for r in payload:
            lat, lon = r.get("latitude"), r.get("longitude")
            if lat is None or lon is None:
                continue
            props = {k: v for k, v in r.items() if k not in ("latitude", "longitude")}
            fc["features"].append({
                "type": "Feature",
                "properties": props,
                "geometry": {"type": "Point", "coordinates": [lon, lat]}
            })
        return Response(json.dumps(fc, indent=2), mimetype="application/json")

    # default list of objects
    return Response(json.dumps({"count": len(payload), "items": payload}, indent=2),
                    mimetype="application/json")

# ---------- Filters ----------
def add_filters(sql: str, params: List[Any], args) -> Tuple[str, List[Any]]:
    # free-text q across street/city/postal
    q = args.get("q")
    if q:
        like = f"%{q}%"
        sql += " AND (address LIKE ? OR city LIKE ? OR REPLACE(postal,' ','') LIKE REPLACE(?,' ',''))"
        params += [like, like, q]

    # Postal prefix/full (ignore spaces)
    postal = args.get("postal")
    if postal:
        postal = clean_postal(postal)
        sql += " AND REPLACE(postal,' ','') LIKE ?"
        params.append(postal + "%")

    # City / Agent / Broker substring
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

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/v1/cities")
def list_cities():
    with connect() as con:
        rows = con.execute(f"""
            SELECT DISTINCT city FROM {TABLE}
            WHERE city IS NOT NULL AND TRIM(city) <> ''
            ORDER BY city ASC
        """).fetchall()
    return jsonify([r["city"] for r in rows])

@app.get("/api/v1/search")
def api_search():
    """
    One flexible search endpoint for the frontend form.
    Optional query params:
      q, city, postal, agent, broker, state, limit (default 50), page (default 1), view=json|map|geojson
    """
    args = request.args
    view  = args.get("view", "json")
    limit = max(1, min(parse_int(args.get("limit"), 50), 200))
    page  = max(1, parse_int(args.get("page"), 1))
    offset = (page - 1) * limit

    sql = f"SELECT rowid AS id, * FROM {TABLE} WHERE 1=1"

    params: List[Any] = []
    sql, params = add_filters(sql, params, args)

    # Default sort: latest inserted first (if you have 'id'); otherwise omit ORDER BY
    sql += " ORDER BY id DESC"
    sql += " LIMIT ? OFFSET ?"
    params += [limit, offset]

    with connect() as con:
        rows = rows_to_dicts(con.execute(sql, tuple(params)).fetchall())

    return respond(rows, view)

# Local dev runner (safe to keep; ignored in production with gunicorn)
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port, debug=True)
