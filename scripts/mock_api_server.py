import os, sys, json, glob, argparse
from pathlib import Path
from flask import Flask, jsonify, request

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = None
for c in [PROJECT_ROOT / "data" / "api_json", PROJECT_ROOT / "generated_data" / "api_json"]:
    if c.exists():
        DATA_DIR = c
        break
if DATA_DIR is None:
    DATA_DIR = PROJECT_ROOT / "data" / "api_json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
_DB = {}
_SV = {}

def load():
    global _DB, _SV
    print(f"\nLoading data from: {DATA_DIR}")
    for f in sorted(glob.glob(str(DATA_DIR / "taichinh_HK*.json"))):
        stem = Path(f).stem
        key = "-".join(stem[len("taichinh_"):].split("_"))
        with open(f, "r", encoding="utf-8") as fh:
            recs = json.load(fh)
        _DB[key] = recs
        print(f"Loaded: {Path(f).name} → {key} ({len(recs)} records)")
    af = DATA_DIR / "taichinh_all.json"
    if af.exists():
        with open(af, "r", encoding="utf-8") as fh:
            _DB["_all"] = json.load(fh)
        print(f"Loaded: taichinh_all.json ({len(_DB['_all'])} records)")
    hks = sorted(k for k in _DB if not k.startswith("_"))
    total = sum(len(v) for k, v in _DB.items() if not k.startswith("_"))
    print(f"Tổng: {len(hks)} học kỳ | {total} records")
    for hk, recs in _DB.items():
        if hk.startswith("_"): continue
        for r in recs:
            ma = str(r.get("ma_sinh_vien", "")).upper().strip()
            if ma:
                _SV.setdefault(ma, []).append(r)
    print(f"Index: {len(_SV)} sinh viên")

load()

@app.route("/health")
def health():
    hks = sorted(k for k in _DB if not k.startswith("_"))
    return jsonify({"status": "ok", "hoc_ky": hks, "total_sv": len(_SV)})

@app.route("/api/tai-chinh/sinh-vien")
def by_hk():
    hk = request.args.get("hoc_ky", "").strip().replace("_", "-").upper()
    if not hk:
        all_r = []
        for k, v in _DB.items():
            if not k.startswith("_"): all_r.extend(v)
        return jsonify({"data": all_r, "total": len(all_r), "hoc_ky": "all"})
    recs = _DB.get(hk, [])
    if not recs and "_all" in _DB:
        recs = [r for r in _DB["_all"] if str(r.get("hoc_ky","")).strip().replace("_","-").upper() == hk]
    return jsonify({"data": recs, "total": len(recs), "hoc_ky": hk})

@app.route("/api/tai-chinh/sinh-vien/<ma>")
def by_sv(ma):
    return jsonify({"data": _SV.get(ma.upper().strip(), []), "ma_sinh_vien": ma.upper()})

@app.route("/api/hoc-ky")
def hoc_ky_list():
    return jsonify({"data": sorted(k for k in _DB if not k.startswith("_"))})

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=5055)
    p.add_argument("--host", default="0.0.0.0")
    a = p.parse_args()
    print(f"Running on http://{a.host}:{a.port}")
    app.run(host=a.host, port=a.port)