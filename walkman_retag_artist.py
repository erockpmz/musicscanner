#!/usr/bin/env python3
"""
Normalize Walkman tags from one artist name to another.

Default use here is to change:
    Pretenders -> The Pretenders

It updates these tags when present:
- artist
- albumartist
- author
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from mutagen import File as MutagenFile

AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".alac"
}

DEFAULT_MUSIC_DIR_CANDIDATES = [
    "MUSIC",
    "MUSIC/Tracks",
    "MUSIC/Music",
]


@dataclass
class PlannedChange:
    path: Path
    changes: dict[str, list[str]]


def detect_walkman_root() -> Optional[Path]:
    volumes = Path("/Volumes")
    if not volumes.exists():
        return None

    preferred: list[tuple[int, Path]] = []
    for child in volumes.iterdir():
        if not child.is_dir():
            continue
        name = child.name.lower()
        score = 0
        if "walkman" in name:
            score += 100
        if "sony" in name:
            score += 50
        if "mymusic" in name:
            score += 25
        if score:
            preferred.append((score, child))

    if not preferred:
        return None

    preferred.sort(key=lambda item: (-item[0], item[1].name.lower()))
    return preferred[0][1]


def pick_existing_subdir(root: Path, candidates: list[str]) -> Optional[Path]:
    for rel in candidates:
        candidate = root / rel
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def resolve_music_root(walkman_root: Optional[Path], music_subdir: Optional[str]) -> Path:
    root = walkman_root.resolve() if walkman_root else detect_walkman_root()
    if root is None:
        raise SystemExit("Could not auto-detect the Walkman under /Volumes.")
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Walkman root does not exist or is not a directory: {root}")

    if music_subdir:
        music_root = root / music_subdir
    else:
        music_root = pick_existing_subdir(root, DEFAULT_MUSIC_DIR_CANDIDATES)

    if music_root is None or not music_root.exists() or not music_root.is_dir():
        raise SystemExit(f"Could not find Walkman music folder under: {root}")

    return music_root.resolve()


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


def find_changes(music_root: Path, from_artist: str, to_artist: str) -> list[PlannedChange]:
    planned: list[PlannedChange] = []

    for path in sorted(music_root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in AUDIO_EXTENSIONS:
            continue

        try:
            audio = MutagenFile(str(path), easy=True)
        except Exception:
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
    parser = argparse.ArgumentParser(description="Normalize Walkman artist tags")
    parser.add_argument("--walkman-root", default=None, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument("--music-subdir", default=None, help="Music folder relative to Walkman root, e.g. MUSIC")
    parser.add_argument("--from-artist", required=True, help='Artist tag value to replace, e.g. "Pretenders"')
    parser.add_argument("--to-artist", required=True, help='Replacement artist tag value, e.g. "The Pretenders"')
    parser.add_argument("--dry-run", action="store_true", help="Show what would change")
    parser.add_argument("--apply", action="store_true", help="Actually write changes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.dry_run and not args.apply:
        raise SystemExit("Use --dry-run to preview or --apply to write changes.")
    if args.dry_run and args.apply:
        raise SystemExit("Use either --dry-run or --apply, not both.")

    walkman_root = Path(args.walkman_root).expanduser() if args.walkman_root else None
    music_root = resolve_music_root(walkman_root, args.music_subdir)

    planned = find_changes(music_root, args.from_artist, args.to_artist)

    print(f"Music root: {music_root}")
    print(f"From artist: {args.from_artist}")
    print(f"To artist: {args.to_artist}")
    print(f"Files to change: {len(planned)}")

    for item in planned:
        rel = item.path.relative_to(music_root)
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
