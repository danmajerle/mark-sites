#!/usr/bin/env python3
from pathlib import Path
import csv, json
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
V1_JSON = ROOT / 'data' / 'processed' / 'developments.v1.json'
SUPP_CSV = ROOT / 'data' / 'supplemental' / 'proposed_large_projects.csv'
OUT_JSON = ROOT / 'data' / 'processed' / 'developments.v2.json'
OUT_CSV = ROOT / 'data' / 'processed' / 'developments.v2.csv'
OUT_JS = ROOT / 'site' / 'data.v2.js'

LARGE_UNITS_THRESHOLD = 100

def load_v1():
    if not V1_JSON.exists():
        raise SystemExit('Missing developments.v1.json. Run scripts/build_v1_pipeline.py first.')
    return json.loads(V1_JSON.read_text())

def parse_int(v, d=0):
    try:
        return int(float(str(v).replace(',', '').strip()))
    except Exception:
        return d

def parse_float(v):
    try:
        s=str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None

def load_supplemental():
    rows=[]
    if not SUPP_CSV.exists():
        return rows
    with SUPP_CSV.open() as f:
        r=csv.DictReader(f)
        for row in r:
            units=parse_int(row.get('units_total',0),0)
            if units < LARGE_UNITS_THRESHOLD:
                continue
            status=(row.get('status') or 'Proposed').strip() or 'Proposed'
            rows.append({
                'project_id': (row.get('project_id') or row.get('address') or row.get('project_name') or '').strip(),
                'project_name': (row.get('project_name') or '').strip() or 'Unnamed proposed project',
                'address': (row.get('address') or '').strip(),
                'neighborhood': (row.get('neighborhood') or '').strip(),
                'status': status,
                'units_total': units,
                'units_affordable': None,
                'stories': None,
                'developer': None,
                'permit_case_id': None,
                'source_url': (row.get('source_url') or '').strip(),
                'first_date_received': None,
                'last_date_issued': None,
                'last_final_date': None,
                'permit_count': 0,
                'valuation_total': 0,
                'longitude': parse_float(row.get('longitude')),
                'latitude': parse_float(row.get('latitude')),
                'last_updated': (row.get('last_updated') or datetime.now().date().isoformat()),
                'source_type': 'supplemental_proposed',
                'notes': (row.get('notes') or '').strip()
            })
    return rows

def merge(v1, supp):
    base=[]
    seen=set()
    for d in v1.get('developments',[]):
        x=dict(d)
        x['source_type']='permit_issued'
        x['notes']=x.get('notes','')
        key=(x.get('project_name','').strip().lower(), x.get('address','').strip().lower())
        seen.add(key)
        base.append(x)

    added=0
    for d in supp:
        key=(d.get('project_name','').strip().lower(), d.get('address','').strip().lower())
        if key in seen:
            continue
        seen.add(key)
        base.append(d)
        added += 1

    base.sort(key=lambda x: (x.get('status',''), x.get('units_total',0)), reverse=True)

    k=v1.get('kpis',{})
    proposed_units=sum(max(0,int(x.get('units_total') or 0)) for x in base if x.get('status') in {'Proposed','Approved'} )
    proposed_count=sum(1 for x in base if x.get('status') in {'Proposed','Approved'})
    kpis={
      **k,
      'projects_tracked': len(base),
      'pipeline_units': sum(max(0,int(x.get('units_total') or 0)) for x in base),
      'proposed_or_approved_projects': proposed_count,
      'proposed_or_approved_units': proposed_units,
      'v2_added_supplemental_projects': added,
      'updated_at': datetime.now(tz=timezone.utc).isoformat(),
      'source': 'Denver permits + supplemental proposed large projects'
    }
    return {'kpis':kpis,'developments':base}


def write_csv(payload):
    fields=['project_id','project_name','address','neighborhood','status','units_total','permit_count','valuation_total','first_date_received','last_date_issued','last_final_date','longitude','latitude','developer','permit_case_id','source_url','source_type','notes','last_updated']
    with OUT_CSV.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        for d in payload['developments']:
            w.writerow({k:d.get(k) for k in fields})


def main():
    v1=load_v1()
    supp=load_supplemental()
    payload=merge(v1,supp)
    OUT_JSON.write_text(json.dumps(payload,indent=2))
    write_csv(payload)
    OUT_JS.write_text('window.DENVER_HOUSING_V2 = ' + json.dumps(payload) + ';\\n')
    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_CSV}")
    print(f"Wrote {OUT_JS}")
    print(f"Supplemental rows ingested: {len(supp)}")

if __name__=='__main__':
    main()
