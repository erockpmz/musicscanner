#!/usr/bin/env python3
"""
Walkman cleanup tool

Finds .m4a files on a mounted Walkman and removes them only when a .flac file with
the same base name exists in the same folder.

Example:
    python3 walkman_remove_m4a_when_flac_exists.py --walkman-root '/Volumes/MyMusic' --dry-run
    python3 walkman_remove_m4a_when_flac_exists.py --walkman-root '/Volumes/MyMusic' --delete
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DEFAULT_MUSIC_DIR_CANDIDATES = [
    "MUSIC",
    "MUSIC/Tracks",
    "MUSIC/Music",
]


@dataclass
class Match:
    folder: Path
    stem: str
    m4a_path: Path
    flac_path: Path


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


def find_matches(music_root: Path) -> list[Match]:
    matches: list[Match] = []

    for folder in sorted({p.parent for p in music_root.rglob('*') if p.is_file()}):
        flac_by_stem: dict[str, Path] = {}
        m4a_by_stem: dict[str, Path] = {}

        for path in folder.iterdir():
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix == '.flac':
                flac_by_stem[path.stem] = path
            elif suffix == '.m4a':
                m4a_by_stem[path.stem] = path

        for stem, m4a_path in sorted(m4a_by_stem.items()):
            flac_path = flac_by_stem.get(stem)
            if flac_path:
                matches.append(Match(folder=folder, stem=stem, m4a_path=m4a_path, flac_path=flac_path))

    return matches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Remove .m4a when matching .flac exists on Walkman')
    parser.add_argument('--walkman-root', default=None, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument('--music-subdir', default=None, help="Music folder relative to Walkman root, e.g. MUSIC")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted')
    parser.add_argument('--delete', action='store_true', help='Actually delete matching .m4a files')
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.dry_run and not args.delete:
        raise SystemExit('Use --dry-run to preview or --delete to remove files.')
    if args.dry_run and args.delete:
        raise SystemExit('Use either --dry-run or --delete, not both.')

    walkman_root = Path(args.walkman_root).expanduser() if args.walkman_root else None
    music_root = resolve_music_root(walkman_root, args.music_subdir)
    matches = find_matches(music_root)

    print(f'Music root: {music_root}')
    print(f'Matching .m4a files with same-name .flac in same folder: {len(matches)}')

    for match in matches:
        rel_m4a = match.m4a_path.relative_to(music_root)
        rel_flac = match.flac_path.relative_to(music_root)
        print(f'M4A : {rel_m4a}')
        print(f'FLAC: {rel_flac}')
        print('---')

    if args.delete:
        deleted = 0
        for match in matches:
            match.m4a_path.unlink()
            deleted += 1
        print(f'Deleted {deleted} .m4a files.')
    else:
        print('Dry run only. No files were deleted.')


if __name__ == '__main__':
    main()
