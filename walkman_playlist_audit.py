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
    # De-duplicate by file identity (inode) to avoid duplicates on case-insensitive filesystems.
    unique: dict[tuple[int, int], Path] = {}
    for p in paths:
        try:
            st = p.stat()
            key = (st.st_dev, st.st_ino)
        except OSError:
            key = hash(str(p.resolve()).lower()), 0
        if key not in unique:
            unique[key] = p.resolve()
    return sorted(unique.values())


def parse_entries(playlist: Path) -> list[str]:
    # Read raw bytes so malformed/binary playlist files do not crash audit.
    raw = playlist.read_bytes()
    text = raw.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()

    entries: list[str] = []
    for line in lines:
        item = line.strip().replace("\x00", "")
        if not item or item.startswith("#"):
            continue
        entries.append(item)
    return entries


def resolve_entry(entry: str, playlist: Path) -> Path | None:
    try:
        p = Path(entry)
        if p.is_absolute():
            return p.resolve()
        return (playlist.parent / p).resolve()
    except (ValueError, OSError):
        # Handles malformed entries (e.g., embedded null byte).
        return None


def audit_playlist(playlist: Path, music_root: Path) -> dict[str, object]:
    entries = parse_entries(playlist)
    missing = 0
    outside = 0

    for entry in entries:
        target = resolve_entry(entry, playlist)
        if target is None or not target.exists():
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
    parser.add_argument(
        "--remove-name-duplicates",
        action="store_true",
        help="For duplicate playlist names, keep one preferred copy and delete others",
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
    duplicates_deleted = 0

    if args.remove_name_duplicates:
        grouped: dict[str, list[dict[str, object]]] = {}
        for r in reports:
            grouped.setdefault(str(r["name"]).lower(), []).append(r)

        for _, group in grouped.items():
            if len(group) < 2:
                continue

            def score(row: dict[str, object]) -> tuple[int, int, int]:
                # Lower score is better.
                path = Path(str(row["file"]))
                in_music = int(path.parent.resolve() == music_root)
                issues = int(row["missing_entries"]) + int(row["outside_music_root"]) + (100 if bool(row["is_appledouble"]) else 0)
                # Prefer in MUSIC folder and fewer issues.
                return (-in_music, issues, len(str(path)))

            keep = min(group, key=score)
            for row in group:
                if row is keep:
                    continue
                file_path = Path(str(row["file"]))
                if file_path.exists():
                    file_path.unlink()
                    duplicates_deleted += 1

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
    if duplicates_deleted:
        print(f"Deleted duplicate-name playlists: {duplicates_deleted}")


if __name__ == "__main__":
    main()
