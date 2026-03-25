#!/usr/bin/env python3
"""
Fast report of tracks missing from a mounted Walkman.

This version is optimized for large libraries and network/cloud folders.
It compares by normalized relative path without opening metadata tags.

Use this when the metadata-based report is too slow.

Examples:
    python3 walkman_missing_report_fast.py \
      --walkman-root "/Volumes/MyMusic" \
      --source-root "/Users/elorson/Library/CloudStorage/Dropbox/Music/Main Catalog" \
      --report-prefix walkman_missing_dropbox_fast
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Optional

AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".alac"
}

DEFAULT_MUSIC_DIR_CANDIDATES = [
    "MUSIC",
    "MUSIC/Tracks",
    "MUSIC/Music",
]


def pick_existing_subdir(root: Path, candidates: list[str]) -> Optional[Path]:
    for rel in candidates:
        candidate = root / rel
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def resolve_walkman_music_root(walkman_root: Path, music_subdir: Optional[str]) -> Path:
    root = walkman_root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Walkman root does not exist or is not a directory: {root}")

    if music_subdir:
        music_root = root / music_subdir
    else:
        music_root = pick_existing_subdir(root, DEFAULT_MUSIC_DIR_CANDIDATES)

    if music_root is None or not music_root.exists() or not music_root.is_dir():
        raise SystemExit(f"Could not find Walkman music folder under: {root}")

    return music_root.resolve()


def normalize_rel_path(path: Path, root: Path) -> str:
    rel = str(path.relative_to(root).with_suffix(""))
    return rel.replace("\\", "/").lower()


def scan_library_fast(root: Path) -> tuple[list[dict], set[str]]:
    rows: list[dict] = []
    keys: set[str] = set()

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in AUDIO_EXTENSIONS:
            continue

        key = normalize_rel_path(path, root)
        rel_path = str(path.relative_to(root))
        rows.append({
            "root": str(root),
            "path": str(path),
            "rel_path": rel_path,
            "ext": path.suffix.lower(),
            "key": key,
        })
        keys.add(key)

    return rows, keys


def load_sources(source_roots: list[str]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[str] = set()

    for raw in source_roots:
        root = Path(raw).expanduser().resolve()
        if not root.exists() or not root.is_dir():
            print(f"Skipping missing source root: {root}")
            continue
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        resolved.append(root)

    if not resolved:
        raise SystemExit("No valid source roots were found.")
    return resolved


def write_reports(prefix: str, walkman_root: Path, walkman_music_root: Path, sources: list[Path], missing: list[dict]) -> tuple[Path, Path, Path]:
    txt_path = Path(f"{prefix}.txt").resolve()
    csv_path = Path(f"{prefix}.csv").resolve()
    json_path = Path(f"{prefix}.json").resolve()

    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write(f"Walkman root: {walkman_root}\n")
        fh.write(f"Walkman music root: {walkman_music_root}\n")
        fh.write("Sources:\n")
        for source_root in sources:
            fh.write(f"  - {source_root}\n")
        fh.write(f"\nMissing tracks: {len(missing)}\n\n")

        current_root = None
        for row in missing:
            if row["root"] != current_root:
                current_root = row["root"]
                fh.write(f"=== {current_root} ===\n")
            fh.write(f'{row["rel_path"]}\n')

        if not missing:
            fh.write("No missing tracks found.\n")

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["root", "rel_path", "ext", "path"],
        )
        writer.writeheader()
        for row in missing:
            writer.writerow({
                "root": row["root"],
                "rel_path": row["rel_path"],
                "ext": row["ext"],
                "path": row["path"],
            })

    payload = {
        "walkman_root": str(walkman_root),
        "walkman_music_root": str(walkman_music_root),
        "sources": [str(root) for root in sources],
        "missing_count": len(missing),
        "missing_tracks": missing,
        "comparison_mode": "normalized relative path without extension",
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    return txt_path, csv_path, json_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast report of tracks missing from Walkman")
    parser.add_argument("--walkman-root", required=True, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument("--music-subdir", default=None, help="Music folder relative to Walkman root, e.g. MUSIC")
    parser.add_argument("--source-root", action="append", default=[], help="Source music root to compare against. Use more than once.")
    parser.add_argument("--report-prefix", default="walkman_missing_fast", help="Output filename prefix")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    walkman_root = Path(args.walkman_root)
    walkman_music_root = resolve_walkman_music_root(walkman_root, args.music_subdir)
    sources = load_sources(args.source_root)

    print(f"Scanning Walkman: {walkman_music_root}")
    _, walkman_keys = scan_library_fast(walkman_music_root)
    print(f"Walkman keys: {len(walkman_keys)}")

    missing: list[dict] = []
    for source_root in sources:
        print(f"Scanning source: {source_root}")
        source_rows, _ = scan_library_fast(source_root)
        source_missing = [row for row in source_rows if row["key"] not in walkman_keys]
        print(f"Missing from this source: {len(source_missing)}")
        missing.extend(source_missing)

    txt_path, csv_path, json_path = write_reports(
        prefix=args.report_prefix,
        walkman_root=walkman_root.resolve(),
        walkman_music_root=walkman_music_root,
        sources=sources,
        missing=missing,
    )

    print(f"Total missing tracks: {len(missing)}")
    print(f"Text report: {txt_path}")
    print(f"CSV report : {csv_path}")
    print(f"JSON report: {json_path}")


if __name__ == "__main__":
    main()
