import json
import logging

from flask import Blueprint, Response, jsonify, request

from database import connect_details
from utils import _parse_json_field, parse_int

log = logging.getLogger(__name__)
details_bp = Blueprint("details", __name__)


@details_bp.get("/api/v1/property/details")
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
            for field in ("sales_history", "comparables", "tax_history", "assessed_value_history"):
                row[field] = _parse_json_field(row.get(field), field)
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
