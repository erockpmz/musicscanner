#!/usr/bin/env python3
"""
Report tracks that exist in source libraries but are missing from a mounted Walkman.

This compares the Walkman's music files against one or more source roots and
writes a missing-tracks report in text, CSV, and JSON formats.

Examples:
    python3 walkman_missing_report.py \
      --walkman-root "/Volumes/MyMusic" \
      --source-root "/Users/elorson/Library/CloudStorage/Dropbox/Music/Main Catalog"

    python3 walkman_missing_report.py \
      --walkman-root "/Volumes/MyMusic" \
      --source-root "/Users/elorson/Library/CloudStorage/Dropbox/Music/Main Catalog" \
      --source-root "/Users/elorson/Documents/Ukeysoft/Ukeysoft Apple Music Converter" \
      --report-prefix walkman_missing
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import threading
import unicodedata
from dataclasses import asdict, dataclass
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
class Track:
    source_name: str
    root: str
    path: str
    rel_path: str
    ext: str
    artist: str
    album: str
    title: str
    track_number: str
    path_key: str
    meta_key: str


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_track_number(value: str) -> str:
    value = normalize_text(value)
    if not value:
        return ""
    match = re.match(r"(\d+)", value)
    if match:
        return match.group(1).lstrip("0") or "0"
    return value


def make_path_key(rel_path: str) -> str:
    rel = str(Path(rel_path).with_suffix(""))
    rel = rel.replace("\\", "/")
    return normalize_text(rel)


def make_meta_key(artist: str, album: str, title: str, track_number: str) -> str:
    artist_n = normalize_text(artist)
    album_n = normalize_text(album)
    title_n = normalize_text(title)
    track_n = normalize_track_number(track_number)

    if not artist_n or not title_n:
        return ""

    if album_n and track_n:
        return f"{artist_n}|{album_n}|{track_n}|{title_n}"
    if album_n:
        return f"{artist_n}|{album_n}|{title_n}"
    return f"{artist_n}|{title_n}"


class LibraryScanner:
    def __init__(self, root: Path, source_name: str) -> None:
        self.root = root.resolve()
        self.source_name = source_name
        self._tracks: list[Track] = []
        self._lock = threading.Lock()

    @staticmethod
    def _clean_tag(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return str(value[0]).strip() if value else ""
        return str(value).strip()

    @staticmethod
    def _first_present(tags: dict, *keys: str) -> str:
        for key in keys:
            if key in tags:
                value = LibraryScanner._clean_tag(tags.get(key))
                if value:
                    return value
        return ""

    def _read_metadata(self, file_path: Path) -> tuple[str, str, str, str]:
        artist = ""
        album = ""
        title = ""
        track_number = ""

        try:
            audio = MutagenFile(str(file_path), easy=True)
            if audio and getattr(audio, "tags", None):
                tags = dict(audio.tags)
                artist = self._first_present(tags, "artist", "albumartist")
                album = self._first_present(tags, "album")
                title = self._first_present(tags, "title")
                track_number = self._first_present(tags, "tracknumber")
        except Exception:
            pass

        if not title:
            title = file_path.stem

        return artist, album, title, track_number

    def scan(self) -> list[Track]:
        found: list[Track] = []

        for path in sorted(self.root.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix.lower() not in AUDIO_EXTENSIONS:
                continue

            rel_path = str(path.relative_to(self.root))
            artist, album, title, track_number = self._read_metadata(path)
            found.append(
                Track(
                    source_name=self.source_name,
                    root=str(self.root),
                    path=str(path),
                    rel_path=rel_path,
                    ext=path.suffix.lower(),
                    artist=artist,
                    album=album,
                    title=title,
                    track_number=track_number,
                    path_key=make_path_key(rel_path),
                    meta_key=make_meta_key(artist, album, title, track_number),
                )
            )

        with self._lock:
            self._tracks = found

        return found


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


def load_sources(source_roots: list[str]) -> list[tuple[str, Path]]:
    resolved: list[tuple[str, Path]] = []
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
        resolved.append((root.name, root))

    if not resolved:
        raise SystemExit("No valid source roots were found.")
    return resolved


def compare_tracks(walkman_tracks: list[Track], source_tracks: list[Track]) -> list[Track]:
    walkman_path_keys = {t.path_key for t in walkman_tracks if t.path_key}
    walkman_meta_keys = {t.meta_key for t in walkman_tracks if t.meta_key}

    missing: list[Track] = []
    for track in source_tracks:
        present = False
        if track.path_key and track.path_key in walkman_path_keys:
            present = True
        elif track.meta_key and track.meta_key in walkman_meta_keys:
            present = True

        if not present:
            missing.append(track)

    return missing


def write_reports(prefix: str, walkman_root: Path, walkman_music_root: Path, sources: list[tuple[str, Path]], missing: list[Track]) -> tuple[Path, Path, Path]:
    txt_path = Path(f"{prefix}.txt").resolve()
    csv_path = Path(f"{prefix}.csv").resolve()
    json_path = Path(f"{prefix}.json").resolve()

    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write(f"Walkman root: {walkman_root}\n")
        fh.write(f"Walkman music root: {walkman_music_root}\n")
        fh.write("Sources:\n")
        for source_name, source_root in sources:
            fh.write(f"  - {source_name}: {source_root}\n")
        fh.write(f"\nMissing tracks: {len(missing)}\n\n")

        current_source = None
        for track in missing:
            if track.source_name != current_source:
                current_source = track.source_name
                fh.write(f"=== {current_source} ===\n")
            fh.write(f"{track.rel_path}\n")
            if track.artist or track.title:
                fh.write(f"  artist={track.artist or '-'} | album={track.album or '-'} | title={track.title or '-'} | track={track.track_number or '-'}\n")
        if not missing:
            fh.write("No missing tracks found.\n")

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "source_name", "root", "rel_path", "ext",
                "artist", "album", "title", "track_number", "path"
            ],
        )
        writer.writeheader()
        for track in missing:
            writer.writerow({
                "source_name": track.source_name,
                "root": track.root,
                "rel_path": track.rel_path,
                "ext": track.ext,
                "artist": track.artist,
                "album": track.album,
                "title": track.title,
                "track_number": track.track_number,
                "path": track.path,
            })

    payload = {
        "walkman_root": str(walkman_root),
        "walkman_music_root": str(walkman_music_root),
        "sources": [{"source_name": name, "root": str(root)} for name, root in sources],
        "missing_count": len(missing),
        "missing_tracks": [asdict(track) for track in missing],
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    return txt_path, csv_path, json_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report tracks missing from Walkman")
    parser.add_argument("--walkman-root", required=True, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument("--music-subdir", default=None, help="Music folder relative to Walkman root, e.g. MUSIC")
    parser.add_argument("--source-root", action="append", default=[], help="Source music root to compare against. Use more than once.")
    parser.add_argument("--report-prefix", default="walkman_missing_report", help="Output filename prefix")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    walkman_root = Path(args.walkman_root)
    walkman_music_root = resolve_walkman_music_root(walkman_root, args.music_subdir)
    sources = load_sources(args.source_root)

    walkman_scanner = LibraryScanner(walkman_music_root, "walkman")
    walkman_tracks = walkman_scanner.scan()

    all_source_tracks: list[Track] = []
    for source_name, source_root in sources:
        scanner = LibraryScanner(source_root, source_name)
        tracks = scanner.scan()
        all_source_tracks.extend(tracks)
        print(f"Scanned {len(tracks)} tracks from source: {source_root}")

    print(f"Scanned {len(walkman_tracks)} tracks from Walkman: {walkman_music_root}")

    missing = compare_tracks(walkman_tracks, all_source_tracks)
    txt_path, csv_path, json_path = write_reports(
        prefix=args.report_prefix,
        walkman_root=walkman_root.resolve(),
        walkman_music_root=walkman_music_root,
        sources=sources,
        missing=missing,
    )

    print(f"Missing tracks: {len(missing)}")
    print(f"Text report: {txt_path}")
    print(f"CSV report : {csv_path}")
    print(f"JSON report: {json_path}")


if __name__ == "__main__":
    main()
