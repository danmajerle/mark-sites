#!/usr/bin/env python3
"""
Build v1 Denver housing pipeline from public ArcGIS residential construction permits.

Source:
- City and County of Denver ArcGIS Feature Service
  ODC_DEV_RESIDENTIALCONSTPERMIT_P (layer 316)
"""

from __future__ import annotations
import csv
import json
import math
import urllib.parse
import urllib.request
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "data" / "processed"

SERVICE = "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_RESIDENTIALCONSTPERMIT_P/FeatureServer/316/query"
PAGE_SIZE = 2000
MIN_ISSUED_WHERE = "DATE_ISSUED >= DATE '2018-01-01'"

FIELDS = [
    "OBJECTID",
    "PERMIT_NUM",
    "LOG_NUM",
    "ADDRESS",
    "NEIGHBORHOOD",
    "UNITS",
    "DATE_RECEIVED",
    "DATE_ISSUED",
    "FINAL_DATE",
    "DATE_CO_ISSUED",
    "CANCEL",
    "CLASS",
    "VALUATION",
    "CONTRACTOR_NAME",
]


def fetch_page(offset: int) -> dict:
    params = {
        "where": MIN_ISSUED_WHERE,
        "outFields": ",".join(FIELDS),
        "f": "json",
        "resultOffset": str(offset),
        "resultRecordCount": str(PAGE_SIZE),
        "orderByFields": "OBJECTID ASC",
        "returnGeometry": "true",
        "outSR": "4326",
    }
    url = f"{SERVICE}?{urllib.parse.urlencode(params)}"

    attempts = 0
    while True:
        attempts += 1
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception:
            if attempts >= 5:
                raise
            time.sleep(1.5 * attempts)


def parse_int(value, default=0):
    if value is None:
        return default
    try:
        if isinstance(value, str):
            v = value.strip().replace(",", "")
            if not v:
                return default
            return int(float(v))
        return int(value)
    except Exception:
        return default


def parse_float(value, default=0.0):
    if value is None:
        return default
    try:
        if isinstance(value, str):
            v = value.strip().replace(",", "")
            if not v:
                return default
            return float(v)
        return float(value)
    except Exception:
        return default


def ts_to_date(ms):
    if ms in (None, "", 0):
        return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date().isoformat()
    except Exception:
        return ""


def infer_status(row: dict) -> str:
    cancel = str(row.get("CANCEL") or "").strip().upper()
    final_date = row.get("FINAL_DATE")
    co = str(row.get("DATE_CO_ISSUED") or "").strip()
    issued = row.get("DATE_ISSUED")
    received = row.get("DATE_RECEIVED")

    if cancel in {"Y", "YES", "TRUE", "1"}:
        return "Cancelled"
    if final_date or co:
        return "Delivered"
    if issued:
        return "Under Construction"
    if received:
        return "Approved"
    return "Proposed"


def status_rank(s: str) -> int:
    # choose "most progressed" status across permits in a grouped project
    order = {
        "Cancelled": 0,
        "Proposed": 1,
        "Approved": 2,
        "Under Construction": 3,
        "Delivered": 4,
    }
    return order.get(s, 0)


def build():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_features = []
    offset = 0
    while True:
        payload = fetch_page(offset)
        print(f"Fetched page offset={offset}", flush=True)
        if payload.get("error"):
            raise RuntimeError(f"ArcGIS error: {payload['error']}")
        feats = payload.get("features", [])
        if not feats:
            break
        all_features.extend(feats)
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    fetched_at = datetime.now(tz=timezone.utc).isoformat()

    raw_path = RAW_DIR / "denver_residential_construction_permits.raw.json"
    raw_path.write_text(json.dumps({"fetched_at": fetched_at, "count": len(all_features), "features": all_features}, indent=2))

    grouped = {}

    for feat in all_features:
        a = feat.get("attributes", {})
        permit_num = (a.get("PERMIT_NUM") or "").strip()
        log_num = (a.get("LOG_NUM") or "").strip()
        addr = (a.get("ADDRESS") or "").strip()
        neighborhood = (a.get("NEIGHBORHOOD") or "").strip()

        # project rollup key: prefer LOG_NUM, then address, then permit
        key = log_num or addr or permit_num
        if not key:
            continue

        status = infer_status(a)
        units = parse_int(a.get("UNITS"), 0)
        valuation = parse_float(a.get("VALUATION"), 0.0)
        date_received = ts_to_date(a.get("DATE_RECEIVED"))
        date_issued = ts_to_date(a.get("DATE_ISSUED"))
        final_date = ts_to_date(a.get("FINAL_DATE"))

        geom = feat.get("geometry") or {}
        lon = geom.get("x")
        lat = geom.get("y")

        if key not in grouped:
            grouped[key] = {
                "project_id": key,
                "project_name": addr if addr else f"Residential project {key}",
                "address": addr,
                "neighborhood": neighborhood,
                "status": status,
                "units_total": 0,
                "units_affordable": None,
                "stories": None,
                "developer": (a.get("CONTRACTOR_NAME") or "").strip() or None,
                "permit_case_id": key,
                "source_url": "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_RESIDENTIALCONSTPERMIT_P/FeatureServer",
                "first_date_received": date_received,
                "last_date_issued": date_issued,
                "last_final_date": final_date,
                "permit_count": 0,
                "valuation_total": 0.0,
                "longitude": None,
                "latitude": None,
                "_lon_sum": 0.0,
                "_lat_sum": 0.0,
                "_coord_count": 0,
                "permits": [],
                "last_updated": datetime.now().date().isoformat(),
            }

        g = grouped[key]
        g["permit_count"] += 1
        g["units_total"] += units
        g["valuation_total"] += valuation
        if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
            g["_lon_sum"] += float(lon)
            g["_lat_sum"] += float(lat)
            g["_coord_count"] += 1

        # keep most progressed status
        if status_rank(status) > status_rank(g["status"]):
            g["status"] = status

        # earliest/ latest dates
        if date_received and (not g["first_date_received"] or date_received < g["first_date_received"]):
            g["first_date_received"] = date_received
        if date_issued and (not g["last_date_issued"] or date_issued > g["last_date_issued"]):
            g["last_date_issued"] = date_issued
        if final_date and (not g["last_final_date"] or final_date > g["last_final_date"]):
            g["last_final_date"] = final_date

        g["permits"].append({
            "permit_num": permit_num,
            "class": a.get("CLASS"),
            "units": units,
            "date_received": date_received,
            "date_issued": date_issued,
            "final_date": final_date,
            "cancel": a.get("CANCEL"),
        })

    developments = [d for d in grouped.values() if d["units_total"] > 0]
    for d in developments:
        if d.get("_coord_count", 0) > 0:
            d["longitude"] = round(d["_lon_sum"] / d["_coord_count"], 6)
            d["latitude"] = round(d["_lat_sum"] / d["_coord_count"], 6)
        d.pop("_lon_sum", None)
        d.pop("_lat_sum", None)
        d.pop("_coord_count", None)
    developments.sort(key=lambda d: (d["status"], d["units_total"], d["permit_count"]), reverse=True)

    kpis = {
        "projects_tracked": len(developments),
        "pipeline_units": sum(max(0, d["units_total"]) for d in developments),
        "delivered_units": sum(max(0, d["units_total"]) for d in developments if d["status"] == "Delivered"),
        "under_construction_units": sum(max(0, d["units_total"]) for d in developments if d["status"] == "Under Construction"),
        "updated_at": fetched_at,
        "source": "Denver ArcGIS ODC_DEV_RESIDENTIALCONSTPERMIT_P",
        "notes": [
            "v1 pipeline infers project-level status from permit lifecycle fields.",
            "Current source is issued residential construction permits (2018+), so v1 mostly represents under-construction and delivered stages.",
            "Units are aggregated from permit rows grouped by LOG_NUM/address fallback.",
            "This is a practical proxy for development pipeline, not a legal entitlement ledger.",
        ],
    }

    out_json = {"kpis": kpis, "developments": developments}

    json_path = OUT_DIR / "developments.v1.json"
    json_path.write_text(json.dumps(out_json, indent=2))

    csv_path = OUT_DIR / "developments.v1.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "project_id",
            "project_name",
            "address",
            "neighborhood",
            "status",
            "units_total",
            "permit_count",
            "valuation_total",
            "first_date_received",
            "last_date_issued",
            "last_final_date",
            "longitude",
            "latitude",
            "developer",
            "permit_case_id",
            "source_url",
            "last_updated",
        ])
        for d in developments:
            w.writerow([
                d["project_id"], d["project_name"], d["address"], d["neighborhood"], d["status"],
                d["units_total"], d["permit_count"], round(d["valuation_total"], 2), d["first_date_received"],
                d["last_date_issued"], d["last_final_date"], d["longitude"], d["latitude"], d["developer"], d["permit_case_id"], d["source_url"], d["last_updated"]
            ])

    # JS bundle for static hosting / file open convenience
    js_path = ROOT / "site" / "data.v1.js"
    js_path.write_text("window.DENVER_HOUSING_V1 = " + json.dumps(out_json) + ";\n")

    print(f"Fetched features: {len(all_features)}")
    print(f"Projects rolled up: {len(developments)}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {js_path}")


if __name__ == "__main__":
    build()
