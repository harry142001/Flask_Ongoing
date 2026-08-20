import json
import logging
import re
from typing import Any, Dict, List, Tuple

from config import MOCK_OVERRIDES
from database import REGION_SQL

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_currency(value) -> str:
    if value is None or value == "":
        return ""
    try:
        return "${:,.0f}".format(float(str(value).replace(",", "").replace("$", "")))
    except (ValueError, TypeError):
        return str(value)


def add_dedup_columns(df):
    """Adds the *_clean columns used to detect duplicate listings.
    Mutates and returns df for convenience."""
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
    return df


DEDUP_COLUMNS = (
    "address_clean", "city_clean", "province_clean", "postal_clean",
    "price_clean", "agent_clean", "broker_clean", "lat_clean", "lon_clean",
)


def parse_int(v, default=None):
    try:
        return int(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def clean_postal(s: str) -> str:
    return (s or "").upper().replace(" ", "")


# ---------------------------------------------------------------------------
# Row transformation helpers
# ---------------------------------------------------------------------------

def rows_to_dicts(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def _full_address(r: Dict[str, Any]) -> str:
    prov = r.get("province") or r.get("state")
    pc = r.get("postcode") or r.get("postal")
    parts = [r.get("address"), r.get("city"), prov, pc]
    return ", ".join(str(p) for p in parts if p and str(p).strip())


def to_api_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    if "state" in out:
        out["province"] = out.pop("state")
    if "postal" in out:
        out["postcode"] = out.pop("postal")
    for key, value in out.items():
        if value != value:  # NaN check
            out[key] = ""
        elif isinstance(value, str) and value.strip().lower() == "nan":
            out[key] = ""
    return out


# ---------------------------------------------------------------------------
# Details / comparables helpers
# ---------------------------------------------------------------------------

def _parse_json_field(value, field_name: str):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            log.warning("Could not parse JSON for field '%s'", field_name)
            return []
    currency_keys = ("sale_price", "price", "tax_estimate", "phased_in_assessment")

    if isinstance(value, list):
        result = []
        for e in value:
            if isinstance(e, dict):
                cleaned = {k: ("" if v is None else v) for k, v in e.items()}
                for key in currency_keys:
                    if key in cleaned and cleaned[key] != "":
                        cleaned[key] = format_currency(cleaned[key])
                result.append(cleaned)
            else:
                result.append(e)
        return result
    return []


def _clean_details(row: Dict[str, Any]) -> Dict[str, Any]:
    skip = {
        "address", "city", "province", "state", "postcode", "postal",
        "postal_code", "country", "latitude", "longitude", "price",
        "agent", "broker", "id", "notes", "comparables",
    }
    json_fields = {"sales_history", "tax_history", "assessed_value_history"}
    out = {}
    for key, value in row.items():
        if key.lower() in skip:
            continue
        if key in json_fields:
            value = _parse_json_field(value, key)
        elif value is None:
            value = ""
        if key == "assessed_value" and value:
            value = format_currency(value)
        out[key] = value
    return out


MLS_DETAIL_FIELDS = (
    "bedrooms", "bathrooms", "square_footage", "property_type", "parking",
    "mls_number", "lot_size", "taxes", "description", "url", "flooring",
    "cooling", "heating_type", "pool_type", "total_parking_spaces", "storeys",
    "sqft_range", "community_name", "title", "age_of_building", "time_on_realtor",
)


def has_details(row: Dict[str, Any], cache: dict) -> bool:
    """True if there's anything worth fetching from the detail endpoint for
    this property — either a Teranet report, or any filled-in MLS field."""
    addr = (row.get("address") or "").lower().strip()
    if addr and addr in cache["property_details"]:
        return True
    return any((row.get(field) or "") != "" for field in MLS_DETAIL_FIELDS)


def _apply_mock_overrides(record: Dict[str, Any]) -> Dict[str, Any]:
    # Try matching by PIN first
    pin = (record.get("details") or {}).get("pin", "")
    if pin and pin in MOCK_OVERRIDES:
        record["comparables"] = MOCK_OVERRIDES[pin]["comparables"]
        return record

    def normalize(s):
        return (s or "").strip().lower()

    composite = f"{normalize(record.get('address'))}|{normalize(record.get('city'))}|{normalize(record.get('province'))}"
    if composite in MOCK_OVERRIDES:
        record["comparables"] = MOCK_OVERRIDES[composite]["comparables"]
    return record


def _attach_details(rows: List[Dict[str, Any]], cache: dict) -> List[Dict[str, Any]]:
    """Moves everything past the core top-level fields into `details`, for
    every property — keeps the top-level schema identical across all records
    (Eric's request: Curbside shouldn't have to branch on two schemas)."""
    for r in rows:
        property_key = r.pop("property_key", "") or ""
        mls_details = {field: r.pop(field, "") or "" for field in MLS_DETAIL_FIELDS}

        addr = (r.get("address") or "").lower().strip()
        detail_row = cache["property_details"].get(addr) if addr else None
        if detail_row:
            report_details = _clean_details(detail_row)
            # Report values win when present; MLS fills in anything the report left blank.
            merged = {
                "property_key": property_key,
                **mls_details,
                **{k: v for k, v in report_details.items() if v != ""},
            }
            r["comparables"] = _parse_json_field(detail_row.get("comparables"), "comparables")
        else:
            merged = {"property_key": property_key, **mls_details}
            r["comparables"] = []

        r["details"] = {k: v for k, v in merged.items() if v not in ("", [], None)}

        r = _apply_mock_overrides(r)

        for comp in r.get("comparables", []):
            if isinstance(comp, dict) and comp.get("compsaleamount"):
                comp["compsaleamount"] = format_currency(comp["compsaleamount"])

    return rows


# ---------------------------------------------------------------------------
# Query building
# ---------------------------------------------------------------------------

def add_filters(sql: str, params: List[Any], args) -> Tuple[str, List[Any]]:
    q = args.get("q")
    if q:
        q = q.strip()
        quoted = [m.group(1).strip() for m in re.finditer(r'"(.*?)"', q)]
        free = re.sub(r'".*?"', " ", q).strip()

        for phrase in quoted:
            if not phrase:
                continue
            like = f"%{phrase}%"
            sql += (
                " AND (address LIKE ? COLLATE NOCASE"
                " OR city LIKE ? COLLATE NOCASE"
                f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                " OR agent LIKE ? COLLATE NOCASE"
                " OR broker LIKE ? COLLATE NOCASE"
                " OR CAST(latitude AS TEXT) LIKE ?"
                " OR CAST(longitude AS TEXT) LIKE ?"
                " OR REPLACE(postal,' ','') LIKE REPLACE(?,' ',''))"
            )
            params += [like] * 8

        if free:
            for t in re.split(r"[,\s()]+", free):
                if not t:
                    continue
                like = f"%{t}%"
                token_clean = clean_postal(t)
                is_numberish = bool(re.fullmatch(r"-?\d+(\.\d+)?", t))
                latlon_like = (t + "%") if is_numberish else like
                is_fsa = bool(re.fullmatch(r"[A-Z]\d[A-Z]", token_clean))
                is_full_postal = bool(re.fullmatch(r"[A-Z]\d[A-Z]\d[A-Z]\d", token_clean))
                is_zip5 = bool(re.fullmatch(r"\d{5}", t))
                is_zip9 = bool(re.fullmatch(r"\d{5}-\d{4}", t))

                sql += (
                    " AND (address LIKE ? COLLATE NOCASE"
                    " OR city LIKE ? COLLATE NOCASE"
                    f" OR {REGION_SQL} LIKE ? COLLATE NOCASE"
                    " OR agent LIKE ? COLLATE NOCASE"
                    " OR broker LIKE ? COLLATE NOCASE"
                    " OR CAST(latitude AS TEXT) LIKE ?"
                    " OR CAST(longitude AS TEXT) LIKE ?"
                    " OR REPLACE(postal,' ','') LIKE ?)"
                )
                if is_fsa or is_full_postal:
                    params += [like, like, like, like, like, latlon_like, latlon_like, token_clean + "%"]
                elif is_zip5 or is_zip9:
                    params += [like, like, like, like, like, latlon_like, latlon_like, t.split("-")[0] + "%"]
                else:
                    params += [like, like, like, like, like, latlon_like, latlon_like, like]

    addr = args.get("address")
    if addr:
        if addr.isdigit():
            sql += " AND address LIKE ? COLLATE NOCASE"
            params.append(f"{addr} %")
        else:
            sql += " AND REPLACE(address,' ','') LIKE ? COLLATE NOCASE"
            params.append(f"%{addr.replace(' ', '')}%")

    for field in ("latitude", "longitude"):
        val = args.get(field)
        if val:
            sql += f" AND CAST({field} AS TEXT) LIKE ?"
            params.append(f"{val.strip()}%")

    postcode = args.get("postcode")
    if postcode:
        sql += " AND REPLACE(postal,' ','') LIKE ?"
        params.append(clean_postal(postcode) + "%")

    city = args.get("city")
    if city:
        sql += " AND city LIKE ?"
        params.append(f"%{city}%")

    for field in ("agent", "broker"):
        val = args.get(field)
        if val:
            sql += f" AND {field} LIKE ?"
            params.append(f"%{val}%")

    region = args.get("province") or args.get("state")
    if region:
        sql += f" AND UPPER({REGION_SQL}) LIKE UPPER(?)"
        params.append(f"{region.strip()}%")

    min_price = args.get("min_price")
    if min_price:
        try:
            sql += " AND price IS NOT NULL AND price != '' AND CAST(REPLACE(REPLACE(REPLACE(price, '$', ''), ',', ''), ' ', '') AS REAL) >= ?"
            params.append(float(min_price))
        except ValueError:
            pass

    max_price = args.get("max_price")
    if max_price:
        try:
            sql += " AND price IS NOT NULL AND price != '' AND CAST(REPLACE(REPLACE(REPLACE(price, '$', ''), ',', ''), ' ', '') AS REAL) <= ?"
            params.append(float(max_price))
        except ValueError:
            pass

    return sql, params


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def respond(payload: List[Dict[str, Any]], view: str = "json"):
    import json
    from flask import Response

    if view == "list":
        out = {
            _full_address(r): f"{r.get('latitude')},{r.get('longitude')}"
            for r in payload
            if r.get("latitude") is not None and r.get("longitude") is not None
        }
        return Response(json.dumps(out, indent=2), status=200, mimetype="application/json")

    return Response(
        json.dumps({"count": len(payload), "items": payload}, indent=2),
        status=200,
        mimetype="application/json",
    )
