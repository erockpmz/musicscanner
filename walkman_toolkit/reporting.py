import csv, json
from pathlib import Path

def write_text(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path

def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path

def write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
