#!/usr/bin/env python3
"""Audit and clean Sony Walkman playlists.

Finds common causes of 'playlist appears in app but not on device':
- macOS AppleDouble files like ._MyHead.m3u
- empty playlists
- entries missing on disk or outside the MUSIC root
"""

from __future__ import annotations

import argparse
from pathlib import Path


def resolve_music_root(walkman_root: Path) -> Path:
    for name in ("MUSIC", "Music", "music"):
        candidate = walkman_root / name
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()
    raise SystemExit(f"Could not find MUSIC folder under: {walkman_root}")


def iter_playlist_files(walkman_root: Path) -> list[Path]:
    paths: list[Path] = []
    for folder_name in ("PLAYLISTS", "Playlists", "playlists", "MUSIC", "Music", "music"):
        folder = walkman_root / folder_name
        if folder.exists() and folder.is_dir():
            paths.extend(sorted(folder.glob("*.m3u")))
    # de-duplicate same path discovered twice
    unique = sorted({p.resolve() for p in paths})
    return unique


def parse_entries(playlist: Path) -> list[str]:
    lines = playlist.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]


def resolve_entry(entry: str, playlist: Path) -> Path:
    p = Path(entry)
    if p.is_absolute():
        return p.resolve()
    return (playlist.parent / p).resolve()


def audit_playlist(playlist: Path, music_root: Path) -> dict[str, object]:
    entries = parse_entries(playlist)
    missing = 0
    outside = 0

    for entry in entries:
        target = resolve_entry(entry, playlist)
        if not target.exists():
            missing += 1
            continue
        try:
            target.relative_to(music_root)
        except ValueError:
            outside += 1

    return {
        "file": str(playlist),
        "name": playlist.stem,
        "is_appledouble": playlist.name.startswith("._"),
        "entries": len(entries),
        "missing_entries": missing,
        "outside_music_root": outside,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and optionally clean Walkman playlists")
    parser.add_argument("--walkman-root", required=True, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument(
        "--remove-appledouble",
        action="store_true",
        help="Delete macOS AppleDouble ._*.m3u files",
    )
    parser.add_argument(
        "--remove-empty",
        action="store_true",
        help="Delete .m3u files with zero playable entries",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    walkman_root = Path(args.walkman_root).expanduser().resolve()
    if not walkman_root.exists():
        raise SystemExit(f"Walkman root does not exist: {walkman_root}")

    music_root = resolve_music_root(walkman_root)
    playlists = iter_playlist_files(walkman_root)
    if not playlists:
        raise SystemExit("No .m3u playlists found in PLAYLISTS or MUSIC folders.")

    reports = [audit_playlist(p, music_root) for p in playlists]

    deleted = 0
    for r in reports:
        file_path = Path(str(r["file"]))
        should_delete = False

        if args.remove_appledouble and bool(r["is_appledouble"]):
            should_delete = True
        if args.remove_empty and int(r["entries"]) == 0:
            should_delete = True

        if should_delete and file_path.exists():
            file_path.unlink()
            deleted += 1

    print(f"Walkman root: {walkman_root}")
    print(f"Music root:   {music_root}")
    print(f"Playlists found: {len(reports)}")
    print()

    for r in reports:
        flag_parts: list[str] = []
        if r["is_appledouble"]:
            flag_parts.append("APPLEDOUBLE")
        if r["entries"] == 0:
            flag_parts.append("EMPTY")
        if r["missing_entries"]:
            flag_parts.append(f"missing={r['missing_entries']}")
        if r["outside_music_root"]:
            flag_parts.append(f"outside_music={r['outside_music_root']}")
        flags = f" [{' | '.join(flag_parts)}]" if flag_parts else ""

        print(f"- {r['name']} -> entries={r['entries']}{flags}")

    if deleted:
        print()
        print(f"Deleted playlists: {deleted}")


if __name__ == "__main__":
    main()
