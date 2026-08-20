from typing import Any, List

import pandas as pd
from flask import Blueprint, jsonify, request

from config import TABLE
from database import connect
from utils import DEDUP_COLUMNS, add_dedup_columns, add_filters, parse_int, rows_to_dicts, to_api_row

analytics_bp = Blueprint("analytics", __name__)


@analytics_bp.get("/api/v1/stats")
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


@analytics_bp.get("/api/v1/data-quality")
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


@analytics_bp.get("/api/v1/duplicates")
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
    df = add_dedup_columns(df)

    prop_keys = ["address_clean", "city_clean", "province_clean", "postal_clean"]
    all_keys = list(DEDUP_COLUMNS)

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
        variant_mask = (
            df.duplicated(subset=prop_keys, keep="first")
            & ~df.duplicated(subset=all_keys, keep="first")
        )
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
