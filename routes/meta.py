from flask import Blueprint, jsonify

from config import TABLE
from database import connect

meta_bp = Blueprint("meta", __name__)


@meta_bp.get("/health")
def health():
    return jsonify({"ok": True}), 200


@meta_bp.get("/api/v1/cities")
def list_cities():
    with connect() as con:
        rows = con.execute(
            f"SELECT DISTINCT city FROM {TABLE} WHERE city IS NOT NULL AND TRIM(city) <> '' ORDER BY city"
        ).fetchall()
    return jsonify([r["city"] for r in rows]), 200
