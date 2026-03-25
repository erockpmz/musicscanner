#!/usr/bin/env python3
"""
Reorganize one artist already on a Sony Walkman into Artist/Album/Song structure.

What it does
------------
- Scans an artist folder on the Walkman.
- Reads tags when available (mutagen), otherwise falls back to filename/path parsing.
- Groups tracks by album.
- Builds/normalizes a folder structure like:
      <WALKMAN_ROOT>/<Artist>/<Album>/<track file>
- Moves files into the new structure.
- Writes a JSON report.

Safety
------
- Dry-run by default.
- No deletions.
- No tag rewriting.
- Only touches files under the chosen artist folder.

Typical use
-----------
Dry run:
python3 artist_reorganize_walkman.py --walkman-root "/Volumes/MyMusic/MUSIC" --artist "Bruce Springsteen"

Apply changes:
python3 artist_reorganize_walkman.py --walkman-root "/Volumes/MyMusic/MUSIC" --artist "Bruce Springsteen" --apply
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


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.casefold().strip()
    value = value.replace("&", " and ")
    value = value.replace("’", "'").replace("`", "'")
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = value.replace("/", " ")
    value = re.sub(r"[^a-z0-9']+", " ", value)
    value = SPACE_RE.sub(" ", value).strip()
    return value


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


def parse_filename_fallback(path: Path, artist_default: str) -> Dict[str, object]:
    stem = path.stem
    title = stem
    tracknumber = None
    discnumber = None

    # patterns like "01 Song", "1-01 Song", "01 - Song"
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
    artist = artist_default
    return {
        "title": title,
        "artist": artist,
        "album": album if album != artist_default else "",
        "tracknumber": tracknumber,
        "discnumber": discnumber,
    }


def read_tags_from_file(path: Path, artist_default: str) -> Dict[str, object]:
    fallback = parse_filename_fallback(path, artist_default)
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
        "artist": first_value(["artist", "albumartist", "\xa9ART", "TPE1", "TPE2"]) or fallback["artist"],
        "album": first_value(["album", "\xa9alb", "TALB"]) or fallback["album"],
        "tracknumber": parse_intish(first_value(["tracknumber", "trkn", "TRCK"])) or fallback["tracknumber"],
        "discnumber": parse_intish(first_value(["discnumber", "disk", "TPOS"])) or fallback["discnumber"],
    }


def scan_artist_files(artist_root: Path, artist_name: str) -> List[TrackInfo]:
    out: List[TrackInfo] = []
    for dirpath, _, filenames in os.walk(artist_root):
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.suffix.lower() not in SUPPORTED_AUDIO_EXTS:
                continue
            tags = read_tags_from_file(path, artist_name)
            out.append(
                TrackInfo(
                    path=path,
                    title=str(tags.get("title") or path.stem),
                    artist=str(tags.get("artist") or artist_name),
                    album=str(tags.get("album") or ""),
                    tracknumber=parse_intish(tags.get("tracknumber")),
                    discnumber=parse_intish(tags.get("discnumber")),
                    ext=path.suffix,
                )
            )
    return out


def build_dest_path(walkman_root: Path, artist_name: str, track: TrackInfo) -> Path:
    artist_dir = safe_name(artist_name)
    album_dir = safe_name(track.album or "Unknown Album")
    if track.discnumber and track.discnumber > 1:
        filename = f"{track.discnumber:02d}-{(track.tracknumber or 0):02d} {safe_name(track.title)}{track.ext}"
    elif track.tracknumber:
        filename = f"{track.tracknumber:02d} {safe_name(track.title)}{track.ext}"
    else:
        filename = f"{safe_name(track.title)}{track.ext}"
    return walkman_root / artist_dir / album_dir / filename


def choose_artist_folder(walkman_root: Path, artist_name: Optional[str]) -> Path:
    if artist_name:
        return walkman_root / artist_name

    dirs = sorted([p for p in walkman_root.iterdir() if p.is_dir()], key=lambda p: p.name.casefold())
    if not dirs:
        raise SystemExit("No artist folders found under walkman root.")
    print("Choose an artist folder:")
    for idx, p in enumerate(dirs, start=1):
        print(f"{idx:3}  {p.name}")
    while True:
        choice = input("Enter number: ").strip()
        if not choice.isdigit():
            continue
        i = int(choice)
        if 1 <= i <= len(dirs):
            return dirs[i - 1]


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


def reorganize_artist(
    walkman_root: Path,
    artist_name_arg: Optional[str],
    apply_changes: bool,
) -> Dict[str, object]:
    artist_root = choose_artist_folder(walkman_root, artist_name_arg)
    artist_name = artist_root.name

    tracks = scan_artist_files(artist_root, artist_name)
    report: Dict[str, object] = {
        "artist": artist_name,
        "artist_root": str(artist_root),
        "total_tracks_found": len(tracks),
        "planned_moves": [],
        "already_ok": [],
        "issues": [],
    }

    for track in tracks:
        dest = build_dest_path(walkman_root, artist_name, track)
        if track.path == dest:
            report["already_ok"].append(str(track.path))
            continue

        if track.path.resolve() == dest.resolve() if dest.exists() else False:
            report["already_ok"].append(str(track.path))
            continue

        planned_dest = unique_dest(dest) if apply_changes and dest.exists() and track.path != dest else dest
        report["planned_moves"].append(
            {
                "from": str(track.path),
                "to": str(planned_dest),
                "album": track.album or "Unknown Album",
                "title": track.title,
                "tracknumber": track.tracknumber,
                "discnumber": track.discnumber,
            }
        )

    if apply_changes:
        for item in report["planned_moves"]:
            src = Path(item["from"])
            dst = Path(item["to"])
            if not src.exists():
                report["issues"].append(f"Missing source during move: {src}")
                continue
            ensure_parent(dst)
            if dst.exists():
                dst = unique_dest(dst)
                item["to"] = str(dst)
            shutil.move(str(src), str(dst))

        # Remove empty directories inside artist root, deepest first.
        all_dirs = sorted([p for p in artist_root.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True)
        for d in all_dirs:
            try:
                d.rmdir()
            except OSError:
                pass

    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Reorganize one artist already on the Walkman into Artist/Album/Song structure.")
    parser.add_argument("--walkman-root", required=True, help='Path to Walkman MUSIC folder, e.g. /Volumes/MyMusic/MUSIC')
    parser.add_argument("--artist", help='Artist folder name under the Walkman root, e.g. "Bruce Springsteen"')
    parser.add_argument("--apply", action="store_true", help="Actually move files. Default is dry-run.")
    parser.add_argument("--report-json", default="artist_reorganize_report.json", help="Where to write the JSON report")
    args = parser.parse_args(argv)

    walkman_root = Path(args.walkman_root).expanduser().resolve()
    if not walkman_root.exists():
        print(f"Walkman root not found: {walkman_root}", file=sys.stderr)
        return 2

    report = reorganize_artist(
        walkman_root=walkman_root,
        artist_name_arg=args.artist,
        apply_changes=args.apply,
    )

    report_path = Path(args.report_json).expanduser().resolve()
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Artist: {report['artist']}")
    print(f"Total tracks found: {report['total_tracks_found']}")
    print(f"Planned moves: {len(report['planned_moves'])}")
    print(f"Already OK: {len(report['already_ok'])}")
    print(f"Issues: {len(report['issues'])}")
    print(f"Apply mode: {args.apply}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
