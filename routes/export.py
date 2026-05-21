import json
from typing import Any, List

import pandas as pd
from flask import Blueprint, Response, request

from config import TABLE
from database import connect
from utils import _full_address, add_filters, parse_int, rows_to_dicts

export_bp = Blueprint("export", __name__)


@export_bp.get("/api/v1/export/csv")
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


@export_bp.get("/api/v1/export/geojson")
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
            if lat != lat or lon != lon:  # NaN check
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
