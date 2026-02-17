# Housing Abundance Denver

A sleek, minimalist website tracking Denver housing delivery with a real data pipeline.

## What v1 does
- Pulls live data from Denver's public ArcGIS **Residential Construction Permits** source (2018+).
- Normalizes and rolls permit rows into project-like records.
- Aggregates unit counts and status signals.
- Publishes outputs for the site and spreadsheet analysis.

## What v2 adds
- Merges permit-derived inventory with a supplemental file for **large proposed/approved projects**.
- Supplemental source file:
  - `data/supplemental/proposed_large_projects.csv`
- Build v2 outputs:
  - `data/processed/developments.v2.json`
  - `data/processed/developments.v2.csv`
  - `site/data.v2.js`

## Data source (v1)
- ArcGIS layer: `ODC_DEV_RESIDENTIALCONSTPERMIT_P` (City and County of Denver)
- URL: https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_RESIDENTIALCONSTPERMIT_P/FeatureServer

## Run pipeline
```bash
cd projects/work/housing-abundance-denver
python3 scripts/build_v1_pipeline.py
```

Outputs:
- `data/raw/denver_residential_construction_permits.raw.json`
- `data/processed/developments.v1.json`
- `data/processed/developments.v1.csv`
- `site/data.v1.js`

## Preview site
```bash
cd projects/work/housing-abundance-denver/site
python3 -m http.server 8787
```
Then open: `http://localhost:8787`

## Deploy (Cloudflare Pages)
See: `docs/deploy-cloudflare-pages.md`

Quick settings:
- Framework preset: None
- Build command: (blank)
- Output directory: `site`

## Current caveats
- v1 source starts at permit issuance, so it mostly captures **under construction** and **delivered** stages.
- "Project" grouping is inferred via `LOG_NUM` with address fallback; this is practical but not a perfect legal-project identifier.
- Next step for fuller pipeline stages: add rezoning/site development feeds (proposed/approved) once we wire authenticated or alternate public endpoints.
