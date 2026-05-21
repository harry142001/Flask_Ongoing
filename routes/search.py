import json
import logging
from typing import Any, List

import pandas as pd
from flask import Blueprint, Response, jsonify, request

from cache import CACHE
from config import TABLE
from database import connect
from utils import (
    _attach_details,
    _full_address,
    add_filters,
    parse_int,
    respond,
    rows_to_dicts,
    to_api_row,
)

log = logging.getLogger(__name__)
search_bp = Blueprint("search", __name__)


@search_bp.get("/api/v1/search")
def api_search():
    args = request.args
    view = args.get("view", "json")
    limit = parse_int(args.get("limit"))
    page = max(1, parse_int(args.get("page"), 1))
    offset = (page - 1) * (limit or 0)
    include_details = args.get("details", "true").lower() != "false"

    filter_keys = (
        "q", "address", "city", "agent", "broker",
        "postcode", "province", "state", "latitude", "longitude",
        "min_price", "max_price",
    )
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
        rows = _attach_details(rows, CACHE)

    log.info(
        "Cache size: %d, rows_db: %d, rows after transform: %d",
        len(CACHE["properties"]), len(rows_db), len(rows),
    )
    return respond(rows, view)


@search_bp.get("/api/v1/recent")
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


@search_bp.get("/api/v1/search/clean")
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

    dedup_keys = [
        "address_clean", "city_clean", "province_clean", "postal_clean",
        "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean",
    ]

    df = df.sort_values("date_added", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=dedup_keys, keep="first")
    df = df.drop(columns=dedup_keys, errors="ignore")

    rows_clean = [to_api_row(r) for r in df.to_dict(orient="records")]
    for r in rows_clean:
        r["formatted_address"] = _full_address(r)
    rows_clean.sort(key=lambda r: r.get("formatted_address", "").lower())

    if include_details and CACHE["loaded"]:
        rows_clean = _attach_details(rows_clean, CACHE)

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
