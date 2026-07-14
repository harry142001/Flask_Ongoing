import json
import logging
import math
import os

from flask import Blueprint, Response, jsonify, request

from cache import CACHE

log = logging.getLogger(__name__)
comparables_bp = Blueprint("comparables", __name__)


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _parse_price(price_str):
    try:
        return float(str(price_str).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


@comparables_bp.get("/api/v1/suggest")
def suggest():
    q = request.args.get("q", "").strip().lower()
    if len(q) < 2:
        return jsonify([])
    seen = set()
    results = []
    for p in CACHE.get("properties", []):
        addr = p.get("address") or ""
        city = p.get("city") or ""
        key = addr.lower().strip()
        if key in seen:
            continue
        if q in addr.lower() or q in city.lower():
            seen.add(key)
            results.append({
                "address": addr,
                "city": city,
                "postal": p.get("postal") or "",
                "price": p.get("price") or "",
            })
            if len(results) == 8:
                break
    return jsonify(results)


@comparables_bp.get("/comparables")
def comparables_page():
    address = request.args.get("address", "")
    tmpl = os.path.join(os.path.dirname(__file__), "comparables_template.html")
    with open(tmpl, "r", encoding="utf-8") as f:
        html = f.read()
    return Response(
        html.replace("__ADDRESS__", address.replace('"', "&quot;")),
        status=200,
        mimetype="text/html",
    )


@comparables_bp.get("/api/v1/comparables")
def auto_comparables():
    address = request.args.get("address", "").strip()
    if not address:
        return jsonify({"error": "address is required"}), 400

    limit     = int(request.args.get("limit", 99999))
    price_pct = float(request.args.get("price_pct", 5)) / 100
    radius_km = float(request.args.get("radius_km", 1.5))
    delta     = radius_km / 111.0

    subject = None
    addr_lower = address.lower()
    for p in CACHE.get("properties", []):
        if (p.get("address") or "").lower().strip() == addr_lower:
            subject = p
            break

    if not subject:
        return jsonify({"error": f"Property not found: {address}"}), 404

    try:
        sub_lat = float(subject.get("latitude") or 0)
        sub_lon = float(subject.get("longitude") or 0)
    except (ValueError, TypeError):
        return jsonify({"error": "Subject property has no coordinates"}), 422

    if sub_lat == 0.0 and sub_lon == 0.0:
        return jsonify({"error": "Subject property has no coordinates"}), 422

    sub_price = _parse_price(subject.get("price"))
    sub_fsa = (subject.get("postal") or "").upper().replace(" ", "")[:3]

    seen_addresses = {}
    for p in CACHE.get("properties", []):
        if (p.get("address") or "").lower().strip() == addr_lower:
            continue

        # Skip properties with no coordinates or NaN
        try:
            lat = float(p.get("latitude") or 0)
            lon = float(p.get("longitude") or 0)
        except (ValueError, TypeError):
            continue

        if lat == 0.0 and lon == 0.0:
            continue
        if lat != lat or lon != lon:
            continue

        # Bounding box filter — replaces FSA filter
        if not (sub_lat - delta <= lat <= sub_lat + delta and
                sub_lon - delta <= lon <= sub_lon + delta):
            continue

        dist = _haversine_km(sub_lat, sub_lon, lat, lon)

        # Price filter
        comp_price = _parse_price(p.get("price"))
        if sub_price and comp_price and sub_price > 0:
            if abs(comp_price - sub_price) / sub_price > price_pct:
                continue

        addr_key = (p.get("address") or "").lower().strip()
        if addr_key not in seen_addresses or dist < seen_addresses[addr_key]["distance_km"]:
            seen_addresses[addr_key] = {
                "property_location":            p.get("address", ""),
                "city":                         p.get("city", ""),
                "postal_code":                  p.get("postal") or p.get("postcode", ""),
                "province":                     p.get("state") or p.get("province", ""),
                "agent":                        p.get("agent", ""),
                "broker":                       p.get("broker", ""),
                "compsaleamount":               p.get("price", ""),
                "distance_km":                  round(dist, 3),
                "latitude":                     lat,
                "longitude":                    lon,
                "comparablesequence":           "",
                "basement_finish_area":         "",
                "lot_depth":                    "",
                "lot_frontage":                 "",
                "municipality":                 p.get("city", ""),
                "number_of_bedrooms":           "",
                "number_of_full_bathrooms":     "",
                "number_of_half_bathrooms":     "",
                "property_type_style":          "",
                "property_type_description":    "",
                "comp_roll":                    "",
                "lot_area":                     "",
                "unit_of_measure_of_lot_area":  "",
                "primary_struc_area_above_grd": "",
                "year_built":                   "",
            }

    candidates = sorted(seen_addresses.values(), key=lambda c: c["distance_km"])
    comparables = candidates[:limit]

    for i, c in enumerate(comparables, 1):
        c["comparablesequence"] = str(i)

    return Response(
        json.dumps({
            "subject": {
                "address":   subject.get("address", ""),
                "city":      subject.get("city", ""),
                "province":  subject.get("state") or subject.get("province", ""),
                "postal":    subject.get("postal") or subject.get("postcode", ""),
                "price":     subject.get("price", ""),
                "fsa":       sub_fsa,
                "latitude":  sub_lat,
                "longitude": sub_lon,
            },
            "count":       len(comparables),
            "radius_km":   radius_km,
            "comparables": comparables,
        }, indent=2),
        status=200,
        mimetype="application/json",
    )