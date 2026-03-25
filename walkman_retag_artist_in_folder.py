#!/usr/bin/env python3
"""
Retag files in one specific artist folder on the Walkman.

This is meant for cleanup cases like:
    Pretenders -> The Pretenders

It only scans the folder you point it at, not the whole Walkman.

Examples:
    python3 walkman_retag_artist_in_folder.py \
      --folder "/Volumes/MyMusic/MUSIC/The Pretenders" \
      --from-artist "Pretenders" \
      --to-artist "The Pretenders" \
      --dry-run

    python3 walkman_retag_artist_in_folder.py \
      --folder "/Volumes/MyMusic/MUSIC/The Pretenders" \
      --from-artist "Pretenders" \
      --to-artist "The Pretenders" \
      --apply
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from mutagen import File as MutagenFile

AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".alac"
}


@dataclass
class PlannedChange:
    path: Path
    changes: dict[str, list[str]]


def clean_value(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    return [text] if text else []


def build_changes(audio, from_artist: str, to_artist: str) -> dict[str, list[str]]:
    tags = getattr(audio, "tags", None)
    if not tags:
        return {}

    changes: dict[str, list[str]] = {}
    for key in ("artist", "albumartist", "author"):
        if key in tags:
            current = clean_value(tags.get(key))
            if current and any(v == from_artist for v in current):
                updated = [to_artist if v == from_artist else v for v in current]
                if updated != current:
                    changes[key] = updated
    return changes


def find_changes(folder: Path, from_artist: str, to_artist: str, recursive: bool) -> list[PlannedChange]:
    planned: list[PlannedChange] = []
    paths = folder.rglob("*") if recursive else folder.glob("*")

    for path in sorted(paths):
        if not path.is_file():
            continue
        if path.suffix.lower() not in AUDIO_EXTENSIONS:
            continue

        try:
            audio = MutagenFile(str(path), easy=True)
        except Exception as exc:
            print(f"Skipping unreadable file: {path} ({exc})")
            continue

        if not audio or not getattr(audio, "tags", None):
            continue

        changes = build_changes(audio, from_artist, to_artist)
        if changes:
            planned.append(PlannedChange(path=path, changes=changes))

    return planned


def apply_changes(planned: list[PlannedChange]) -> int:
    changed = 0
    for item in planned:
        audio = MutagenFile(str(item.path), easy=True)
        if not audio or not getattr(audio, "tags", None):
            continue
        for key, values in item.changes.items():
            audio.tags[key] = values
        audio.save()
        changed += 1
    return changed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retag artist fields in one folder")
    parser.add_argument("--folder", required=True, help="Folder to scan, e.g. /Volumes/MyMusic/MUSIC/The Pretenders")
    parser.add_argument("--from-artist", required=True, help='Artist tag value to replace, e.g. "Pretenders"')
    parser.add_argument("--to-artist", required=True, help='Replacement artist tag value, e.g. "The Pretenders"')
    parser.add_argument("--recursive", action="store_true", help="Scan subfolders too")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change")
    parser.add_argument("--apply", action="store_true", help="Actually write changes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.dry_run and not args.apply:
        raise SystemExit("Use --dry-run to preview or --apply to write changes.")
    if args.dry_run and args.apply:
        raise SystemExit("Use either --dry-run or --apply, not both.")

    folder = Path(args.folder).expanduser().resolve()
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"Folder does not exist or is not a directory: {folder}")

    planned = find_changes(folder, args.from_artist, args.to_artist, args.recursive)

    print(f"Folder: {folder}")
    print(f"From artist: {args.from_artist}")
    print(f"To artist: {args.to_artist}")
    print(f"Recursive: {args.recursive}")
    print(f"Files to change: {len(planned)}")

    for item in planned:
        rel = item.path.relative_to(folder)
        print(rel)
        for key, values in item.changes.items():
            print(f"  {key} -> {values}")
        print("---")

    if args.apply:
        changed = apply_changes(planned)
        print(f"Updated {changed} files.")
    else:
        print("Dry run only. No files were changed.")


if __name__ == "__main__":
    main()
