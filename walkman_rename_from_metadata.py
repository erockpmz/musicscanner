#!/usr/bin/env python3
"""
Rename/reorganize Walkman music files from embedded metadata.

Goal
----
Use the tags already inside the audio files to normalize the on-device structure to:

    <WALKMAN_ROOT>/<Artist>/<Album>/<Track Filename>

Default behavior is DRY RUN.
Use --apply to actually move/rename files.

What it does
------------
- Scans audio files under the Walkman root
- Reads embedded tags when available
- Falls back to current folder/file names only when necessary
- Builds destination paths from metadata
- Renames/moves files into Artist/Album folders
- Optionally normalizes filenames to track-number + title

What it does NOT do
-------------------
- No online lookup
- No metadata editing
- No deletions
- No playlist creation
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None

SUPPORTED_AUDIO_EXTS = {".flac", ".m4a", ".mp3", ".wav", ".aiff", ".aac", ".alac"}
IGNORED_FILENAMES = {".DS_Store", "Thumbs.db", "desktop.ini", "Icon\r"}
SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9._\-()'&, ]+")
SPACE_RE = re.compile(r"\s+")


@dataclass
class TrackInfo:
    path: Path
    title: str
    artist: str
    album: str
    tracknumber: Optional[int]
    discnumber: Optional[int]
    ext: str


def safe_name(value: str, limit: int = 120) -> str:
    value = (value or "").strip().replace("/", "-").replace(":", " - ")
    value = SAFE_CHARS_RE.sub("", value)
    value = SPACE_RE.sub(" ", value).strip()
    if not value:
        value = "Unknown"
    return value[:limit].rstrip(" .")


def parse_intish(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    m = re.match(r"(\d+)", text)
    return int(m.group(1)) if m else None


def parse_filename_fallback(path: Path) -> Dict[str, object]:
    stem = path.stem
    title = stem
    tracknumber = None
    discnumber = None

    m = re.match(r"^\s*(?:(\d{1,2})\s*[-._ ]+)?(\d{1,3})\s*[-._ ]+\s*(.+?)\s*$", stem)
    if m:
        discnumber = parse_intish(m.group(1))
        tracknumber = parse_intish(m.group(2))
        title = m.group(3)
    else:
        m = re.match(r"^\s*(\d{1,3})\s*[-._ ]+\s*(.+?)\s*$", stem)
        if m:
            tracknumber = parse_intish(m.group(1))
            title = m.group(2)

    album = path.parent.name if path.parent else ""
    artist = path.parent.parent.name if path.parent and path.parent.parent else ""
    return {
        "title": title,
        "artist": artist,
        "album": album,
        "tracknumber": tracknumber,
        "discnumber": discnumber,
    }


def read_tags(path: Path) -> Dict[str, object]:
    fallback = parse_filename_fallback(path)
    if MutagenFile is None:
        return fallback

    try:
        audio = MutagenFile(path)
    except Exception:
        return fallback
    if audio is None:
        return fallback

    tags = getattr(audio, "tags", None)
    if tags is None:
        return fallback

    def first_value(keys: Sequence[str]) -> Optional[str]:
        for key in keys:
            try:
                value = tags.get(key)
            except Exception:
                continue
            if value is None:
                continue
            if isinstance(value, list):
                if not value:
                    continue
                value = value[0]
            if hasattr(value, "text"):
                text = getattr(value, "text")
                if isinstance(text, list) and text:
                    value = text[0]
            try:
                s = str(value).strip()
            except Exception:
                continue
            if s:
                return s
        return None

    return {
        "title": first_value(["title", "\xa9nam", "TIT2"]) or fallback["title"],
        "artist": first_value(["albumartist", "artist", "\xa9ART", "TPE2", "TPE1"]) or fallback["artist"],
        "album": first_value(["album", "\xa9alb", "TALB"]) or fallback["album"],
        "tracknumber": parse_intish(first_value(["tracknumber", "trkn", "TRCK"])) or fallback["tracknumber"],
        "discnumber": parse_intish(first_value(["discnumber", "disk", "TPOS"])) or fallback["discnumber"],
    }


def should_skip(path: Path) -> bool:
    if path.name in IGNORED_FILENAMES:
        return True
    if path.name.startswith("._"):
        return True
    if path.suffix.lower() not in SUPPORTED_AUDIO_EXTS:
        return True
    return False


def scan_tracks(walkman_root: Path, artist_filter: Optional[str]) -> List[TrackInfo]:
    tracks: List[TrackInfo] = []
    for dirpath, _, filenames in os.walk(walkman_root):
        for filename in filenames:
            path = Path(dirpath) / filename
            if should_skip(path):
                continue
            tags = read_tags(path)
            track = TrackInfo(
                path=path,
                title=str(tags.get("title") or path.stem),
                artist=str(tags.get("artist") or ""),
                album=str(tags.get("album") or ""),
                tracknumber=parse_intish(tags.get("tracknumber")),
                discnumber=parse_intish(tags.get("discnumber")),
                ext=path.suffix.lower(),
            )
            if artist_filter and track.artist != artist_filter and path.parts[-3:-2] != (artist_filter,):
                continue
            tracks.append(track)
    return tracks


def build_filename(track: TrackInfo, include_disc_prefix: bool) -> str:
    title = safe_name(track.title or track.path.stem)
    if include_disc_prefix and track.discnumber and track.discnumber > 1:
        if track.tracknumber is not None:
            return f"{track.discnumber:02d}-{track.tracknumber:02d} {title}{track.ext}"
        return f"{track.discnumber:02d} {title}{track.ext}"
    if track.tracknumber is not None:
        return f"{track.tracknumber:02d} {title}{track.ext}"
    return f"{title}{track.ext}"


def build_destination(walkman_root: Path, track: TrackInfo, include_disc_prefix: bool) -> Path:
    artist_dir = safe_name(track.artist or "Unknown Artist")
    album_dir = safe_name(track.album or "Unknown Album")
    filename = build_filename(track, include_disc_prefix=include_disc_prefix)
    return walkman_root / artist_dir / album_dir / filename


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def unique_dest(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    i = 2
    while True:
        candidate = parent / f"{stem} ({i}){suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def reorganize(
    walkman_root: Path,
    artist_filter: Optional[str],
    include_disc_prefix: bool,
    apply_changes: bool,
) -> Dict[str, object]:
    tracks = scan_tracks(walkman_root, artist_filter=artist_filter)
    report: Dict[str, object] = {
        "walkman_root": str(walkman_root),
        "artist_filter": artist_filter,
        "total_tracks_found": len(tracks),
        "planned_moves": [],
        "already_ok": [],
        "skipped_missing_metadata": [],
        "issues": [],
    }

    for track in tracks:
        if not track.artist or not track.album:
            report["skipped_missing_metadata"].append(str(track.path))
            continue

        dest = build_destination(walkman_root, track, include_disc_prefix=include_disc_prefix)
        if track.path == dest:
            report["already_ok"].append(str(track.path))
            continue

        report["planned_moves"].append(
            {
                "from": str(track.path),
                "to": str(dest),
                "artist": track.artist,
                "album": track.album,
                "title": track.title,
                "tracknumber": track.tracknumber,
                "discnumber": track.discnumber,
            }
        )

    if apply_changes:
        # Move deepest sources first so nested moves don't trip over each other.
        moves = sorted(report["planned_moves"], key=lambda x: len(Path(x["from"]).parts), reverse=True)
        for item in moves:
            src = Path(item["from"])
            dst = Path(item["to"])
            if not src.exists():
                report["issues"].append(f"Missing source during move: {src}")
                continue
            ensure_parent(dst)
            if dst.exists():
                # If same file already there, keep destination and skip source only if identical size.
                try:
                    if src.stat().st_size == dst.stat().st_size:
                        report["issues"].append(f"Destination exists, skipped duplicate-sized source: {src} -> {dst}")
                        continue
                except Exception:
                    pass
                dst = unique_dest(dst)
                item["to"] = str(dst)
            try:
                shutil.move(str(src), str(dst))
            except Exception as exc:
                report["issues"].append(f"Move failed: {src} -> {dst}: {type(exc).__name__}: {exc}")

        # Remove empty directories under walkman root, deepest first.
        all_dirs = sorted([p for p in walkman_root.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True)
        for d in all_dirs:
            try:
                d.rmdir()
            except OSError:
                pass

    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Rename/reorganize Walkman files from embedded metadata.")
    parser.add_argument("--walkman-root", required=True, help='Path to Walkman MUSIC folder, e.g. /Volumes/MyMusic/MUSIC')
    parser.add_argument("--artist", help='Optional: only process one artist, e.g. "The Beatles"')
    parser.add_argument("--no-disc-prefix", action="store_true", help="Do not prefix filenames with disc number on multi-disc albums.")
    parser.add_argument("--apply", action="store_true", help="Actually move/rename files. Default is dry-run.")
    parser.add_argument("--report-json", default="walkman_rename_report.json", help="Where to write the JSON report")
    args = parser.parse_args(argv)

    walkman_root = Path(args.walkman_root).expanduser().resolve()
    if not walkman_root.exists():
        print(f"Walkman root not found: {walkman_root}", file=sys.stderr)
        return 2

    report = reorganize(
        walkman_root=walkman_root,
        artist_filter=args.artist,
        include_disc_prefix=not args.no_disc_prefix,
        apply_changes=args.apply,
    )

    report_path = Path(args.report_json).expanduser().resolve()
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Total tracks found: {report['total_tracks_found']}")
    print(f"Planned moves: {len(report['planned_moves'])}")
    print(f"Already OK: {len(report['already_ok'])}")
    print(f"Skipped missing metadata: {len(report['skipped_missing_metadata'])}")
    print(f"Issues: {len(report['issues'])}")
    print(f"Apply mode: {args.apply}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
