#!/usr/bin/env python3
"""
Conservative music-library noise-file cleaner.

Targets only obvious non-music junk:
- AppleDouble sidecars: ._*
- .DS_Store
- Thumbs.db
- desktop.ini
- Icon\r
- .Spotlight-V100, .Trashes, .fseventsd (directory metadata)
- temporary/editor leftovers like *.tmp, *.part, *.download, *.crdownload

Dry-run by default.
Use --apply to actually delete.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Dict, List

DEFAULT_ROOTS = [
    "/Volumes/MyMusic/MUSIC",
    "/Volumes/music/applemusic",
    "/Users/elorson/Music/VinylStudio/My Albums",
    "/Users/elorson/Music/Rips",
    "/Users/elorson/Music/My Albums",
    "/Users/elorson/Music/Music/Media.localized/Apple Music",
    "/Users/elorson/Documents/Ukeysoft/Ukeysoft Apple Music Converter",
]

NOISE_FILENAMES = {
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
    "Icon\r",
}

NOISE_SUFFIXES = {
    ".tmp",
    ".part",
    ".download",
    ".crdownload",
}

NOISE_DIRS = {
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
}

def is_noise_file(path: Path) -> bool:
    name = path.name
    if name.startswith("._"):
        return True
    if name in NOISE_FILENAMES:
        return True
    if path.suffix.lower() in NOISE_SUFFIXES:
        return True
    return False

def scan_roots(roots: List[str]) -> Dict[str, object]:
    files: List[str] = []
    dirs: List[str] = []
    missing_roots: List[str] = []

    for root_str in roots:
        root = Path(root_str).expanduser()
        if not root.exists():
            missing_roots.append(str(root))
            continue

        for dirpath, dirnames, filenames in os.walk(root, topdown=True):
            current = Path(dirpath)

            removable_dirnames = []
            kept_dirnames = []
            for d in dirnames:
                if d in NOISE_DIRS:
                    dirs.append(str(current / d))
                    removable_dirnames.append(d)
                else:
                    kept_dirnames.append(d)
            dirnames[:] = kept_dirnames

            for fn in filenames:
                p = current / fn
                if is_noise_file(p):
                    files.append(str(p))

    return {
        "files": sorted(files),
        "dirs": sorted(dirs),
        "missing_roots": sorted(missing_roots),
    }

def apply_cleanup(report: Dict[str, object]) -> Dict[str, object]:
    deleted_files = []
    deleted_dirs = []
    errors = []

    for p_str in report["files"]:
        p = Path(p_str)
        try:
            if p.exists():
                p.unlink()
                deleted_files.append(p_str)
        except Exception as exc:
            errors.append(f"FILE {p_str}: {type(exc).__name__}: {exc}")

    # delete deeper dirs first
    dir_paths = sorted((Path(x) for x in report["dirs"]), key=lambda p: len(p.parts), reverse=True)
    for p in dir_paths:
        try:
            if p.exists():
                shutil.rmtree(p)
                deleted_dirs.append(str(p))
        except Exception as exc:
            errors.append(f"DIR {p}: {type(exc).__name__}: {exc}")

    return {
        "deleted_files": deleted_files,
        "deleted_dirs": deleted_dirs,
        "errors": errors,
    }

def main() -> int:
    parser = argparse.ArgumentParser(description="Remove obvious noise files from music locations.")
    parser.add_argument("--apply", action="store_true", help="Actually delete files. Default is dry-run.")
    parser.add_argument("--root", action="append", default=[], help="Override/add roots to scan. Repeat as needed.")
    parser.add_argument("--report-json", default="music_noise_cleanup_report.json", help="Report path.")
    args = parser.parse_args()

    roots = args.root if args.root else DEFAULT_ROOTS
    report = scan_roots(roots)

    output = {
        "roots": roots,
        "missing_roots": report["missing_roots"],
        "noise_files_found": len(report["files"]),
        "noise_dirs_found": len(report["dirs"]),
        "files": report["files"],
        "dirs": report["dirs"],
        "apply_mode": args.apply,
    }

    if args.apply:
        applied = apply_cleanup(report)
        output.update({
            "deleted_files": len(applied["deleted_files"]),
            "deleted_dirs": len(applied["deleted_dirs"]),
            "errors": applied["errors"],
        })

    report_path = Path(args.report_json).expanduser().resolve()
    report_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Roots scanned: {len(roots)}")
    print(f"Missing roots: {len(report['missing_roots'])}")
    print(f"Noise files found: {len(report['files'])}")
    print(f"Noise dirs found: {len(report['dirs'])}")
    print(f"Apply mode: {args.apply}")
    if args.apply:
        print(f"Deleted files: {output['deleted_files']}")
        print(f"Deleted dirs: {output['deleted_dirs']}")
        print(f"Errors: {len(output['errors'])}")
    print(f"Report: {report_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
