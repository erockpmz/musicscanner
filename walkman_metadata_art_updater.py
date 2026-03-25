#!/usr/bin/env python3
"""
Conservative Walkman metadata + album art updater.

Trusted sources only:
- MusicBrainz web service for release / track metadata
- Cover Art Archive for front cover images

Behavior:
- Scans albums already on the Walkman under Artist/Album folders
- Builds a candidate match from local tags / filenames
- Looks up likely releases in MusicBrainz
- Chooses a match only when confidence is high
- Updates tags and embeds front cover art only when publicly available from the
  trusted sources above
- Leaves everything unchanged when confidence is low

Dry-run by default.
Use --apply to actually write changes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import certifi
except Exception:
    certifi = None

try:
    from mutagen import File as MutagenFile
    from mutagen.flac import FLAC, Picture
    from mutagen.id3 import APIC, ID3
    from mutagen.mp4 import MP4, MP4Cover
except Exception:
    MutagenFile = None
    FLAC = None
    Picture = None
    APIC = None
    ID3 = None
    MP4 = None
    MP4Cover = None

APP_NAME = "walkman_metadata_art_updater"
APP_VERSION = "0.1.0"
MUSICBRAINZ_BASE = "https://musicbrainz.org/ws/2"
CAA_BASE = "https://coverartarchive.org"

SUPPORTED_AUDIO_EXTS = {".flac", ".m4a", ".mp3", ".wav", ".aiff", ".aac", ".alac"}
SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9._\-()'&, ]+")
SPACE_RE = re.compile(r"\s+")

# Minimum album confidence before we write anything.
MIN_TRACK_MATCH_RATIO = 0.70
MIN_TRACKS_MATCHED = 4


@dataclass
class LocalTrack:
    path: Path
    title: str
    artist: str
    album: str
    tracknumber: Optional[int]
    discnumber: Optional[int]
    ext: str


@dataclass
class AlbumMatch:
    release_id: str
    release_group_id: Optional[str]
    artist_credit: str
    album_title: str
    date: str
    status: str
    country: str
    track_total: int
    matched_tracks: int
    matched_ratio: float
    confidence: float


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


def normalize_title(value: Optional[str]) -> str:
    value = normalize_text(value)
    value = re.sub(r"\blive\b", " ", value)
    value = re.sub(r"\bremaster(?:ed)?\b", " ", value)
    value = re.sub(r"\bversion\b", " ", value)
    value = SPACE_RE.sub(" ", value).strip()
    return value


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


def mb_get(path: str, **params) -> dict:
    merged = {"fmt": "json"}
    merged.update(params)
    url = f"{MUSICBRAINZ_BASE}/{path}?{urlencode(merged, doseq=True)}"
    req = Request(
        url,
        headers={
            "User-Agent": f"{APP_NAME}/{APP_VERSION} (personal local script)",
            "Accept": "application/json",
        },
    )
    context = ssl.create_default_context(cafile=certifi.where()) if certifi is not None else None
    with urlopen(req, timeout=60, context=context) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    time.sleep(1.05)
    return data


def caa_get_json(path: str) -> dict:
    url = f"{CAA_BASE}/{path}"
    req = Request(
        url,
        headers={
            "User-Agent": f"{APP_NAME}/{APP_VERSION} (personal local script)",
            "Accept": "application/json",
        },
    )
    context = ssl.create_default_context(cafile=certifi.where()) if certifi is not None else None
    with urlopen(req, timeout=60, context=context) as resp:
        return json.loads(resp.read().decode("utf-8"))


def caa_get_bytes(url: str) -> bytes:
    req = Request(
        url,
        headers={"User-Agent": f"{APP_NAME}/{APP_VERSION} (personal local script)"},
    )
    context = ssl.create_default_context(cafile=certifi.where()) if certifi is not None else None
    with urlopen(req, timeout=60, context=context) as resp:
        return resp.read()


def parse_filename_fallback(path: Path, artist_default: str, album_default: str) -> Dict[str, object]:
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

    return {
        "title": title,
        "artist": artist_default,
        "album": album_default,
        "tracknumber": tracknumber,
        "discnumber": discnumber,
    }


def read_tags(path: Path, artist_default: str, album_default: str) -> Dict[str, object]:
    fallback = parse_filename_fallback(path, artist_default, album_default)
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


def scan_album_dirs(walkman_root: Path) -> List[Tuple[Path, str, str]]:
    out = []
    for artist_dir in sorted([p for p in walkman_root.iterdir() if p.is_dir()], key=lambda p: p.name.casefold()):
        if artist_dir.name == "Playlists":
            continue
        for album_dir in sorted([p for p in artist_dir.iterdir() if p.is_dir()], key=lambda p: p.name.casefold()):
            files = [f for f in album_dir.iterdir() if f.is_file() and f.suffix.lower() in SUPPORTED_AUDIO_EXTS and not f.name.startswith("._")]
            if files:
                out.append((album_dir, artist_dir.name, album_dir.name))
    return out


def load_local_album(album_dir: Path, artist_name: str, album_name: str) -> List[LocalTrack]:
    tracks: List[LocalTrack] = []
    for path in sorted(album_dir.iterdir(), key=lambda p: p.name.casefold()):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SUPPORTED_AUDIO_EXTS:
            continue
        if path.name.startswith("._"):
            continue
        tags = read_tags(path, artist_name, album_name)
        tracks.append(
            LocalTrack(
                path=path,
                title=str(tags.get("title") or path.stem),
                artist=str(tags.get("artist") or artist_name),
                album=str(tags.get("album") or album_name),
                tracknumber=parse_intish(tags.get("tracknumber")),
                discnumber=parse_intish(tags.get("discnumber")),
                ext=path.suffix.lower(),
            )
        )
    return tracks


def build_album_query(artist_name: str, album_name: str) -> str:
    return f'release:"{album_name}" AND artist:"{artist_name}"'


def search_releases(artist_name: str, album_name: str) -> List[dict]:
    data = mb_get("release", query=build_album_query(artist_name, album_name), limit=10)
    return data.get("releases", [])


def get_release(release_id: str) -> dict:
    return mb_get(f"release/{release_id}", inc="recordings+artist-credits+release-groups+media")


def local_track_key(t: LocalTrack) -> Tuple[str, Optional[int], Optional[int]]:
    return (normalize_title(t.title), t.tracknumber, t.discnumber)


def remote_track_key(title: str, tracknum: Optional[int], discnum: Optional[int]) -> Tuple[str, Optional[int], Optional[int]]:
    return (normalize_title(title), tracknum, discnum)


def score_release(local_tracks: List[LocalTrack], release: dict) -> AlbumMatch:
    media = release.get("media", [])
    remote_tracks = []
    for medium in media:
        disc = parse_intish(medium.get("position")) or 1
        for idx, tr in enumerate(medium.get("tracks", []), start=1):
            remote_tracks.append(
                (
                    tr.get("title") or "",
                    parse_intish(tr.get("number")) or idx,
                    disc,
                )
            )

    local_keys = {local_track_key(t) for t in local_tracks}
    remote_keys = {remote_track_key(title, trk, disc) for title, trk, disc in remote_tracks}

    # relaxed fallback on title-only
    local_titles = {normalize_title(t.title) for t in local_tracks}
    remote_titles = {normalize_title(title) for title, _, _ in remote_tracks}

    exact_matches = len(local_keys & remote_keys)
    title_matches = len(local_titles & remote_titles)

    matched = max(exact_matches, title_matches)
    remote_total = len(remote_tracks) or 1
    ratio = matched / remote_total

    local_artist = normalize_text(local_tracks[0].artist if local_tracks else "")
    release_artist = normalize_text("".join(ac.get("name", "") + ac.get("joinphrase", "") for ac in release.get("artist-credit", [])))
    artist_bonus = 0.10 if local_artist and local_artist in release_artist else 0.0
    status_bonus = 0.05 if (release.get("status") or "").casefold() == "official" else 0.0
    country_bonus = 0.02 if release.get("country") else 0.0
    confidence = ratio + artist_bonus + status_bonus + country_bonus

    rg = release.get("release-group") or {}
    return AlbumMatch(
        release_id=release["id"],
        release_group_id=rg.get("id"),
        artist_credit="".join(ac.get("name", "") + ac.get("joinphrase", "") for ac in release.get("artist-credit", [])),
        album_title=release.get("title") or "",
        date=release.get("date") or "",
        status=release.get("status") or "",
        country=release.get("country") or "",
        track_total=remote_total,
        matched_tracks=matched,
        matched_ratio=ratio,
        confidence=confidence,
    )


def choose_album_match(local_tracks: List[LocalTrack], artist_name: str, album_name: str) -> Optional[AlbumMatch]:
    candidates = search_releases(artist_name, album_name)
    if not candidates:
        return None

    scored = []
    for c in candidates:
        try:
            full = get_release(c["id"])
        except Exception:
            continue
        scored.append(score_release(local_tracks, full))

    if not scored:
        return None

    scored.sort(key=lambda m: (m.confidence, m.matched_tracks, m.track_total, m.date), reverse=True)
    best = scored[0]
    if best.matched_tracks < MIN_TRACKS_MATCHED:
        return None
    if best.matched_ratio < MIN_TRACK_MATCH_RATIO:
        return None
    return best


def get_release_full(release_id: str) -> dict:
    return mb_get(f"release/{release_id}", inc="recordings+artist-credits+release-groups+media")


def fetch_cover_art(match: AlbumMatch) -> Tuple[Optional[bytes], Optional[str]]:
    # Prefer release-group front art when available, else release-level art.
    tried = []
    if match.release_group_id:
        tried.append(("release-group", match.release_group_id))
    tried.append(("release", match.release_id))

    for entity, mbid in tried:
        try:
            data = caa_get_json(f"{entity}/{mbid}")
        except Exception:
            continue
        images = data.get("images", [])
        for img in images:
            if not img.get("front"):
                continue
            thumb = img.get("thumbnails", {}).get("large") or img.get("image")
            if not thumb:
                continue
            try:
                return caa_get_bytes(thumb), thumb
            except Exception:
                continue
    return None, None


def apply_tags_and_art(track: LocalTrack, release_full: dict, cover_bytes: Optional[bytes]) -> None:
    if MutagenFile is None:
        return

    # map local track to remote track by title then track/disc
    remote_map = {}
    for medium in release_full.get("media", []):
        disc = parse_intish(medium.get("position")) or 1
        for idx, tr in enumerate(medium.get("tracks", []), start=1):
            remote_map[(normalize_title(tr.get("title") or ""), parse_intish(tr.get("number")) or idx, disc)] = tr

    target = remote_map.get((normalize_title(track.title), track.tracknumber, track.discnumber or 1))
    if target is None:
        # title-only fallback
        for key, tr in remote_map.items():
            if key[0] == normalize_title(track.title):
                target = tr
                break
    if target is None:
        return

    audio = MutagenFile(track.path)
    if audio is None:
        return

    album_title = release_full.get("title") or track.album
    artist_credit = "".join(ac.get("name", "") + ac.get("joinphrase", "") for ac in release_full.get("artist-credit", [])) or track.artist
    tr_title = target.get("title") or track.title
    tr_no = parse_intish(target.get("number")) or track.tracknumber
    date = release_full.get("date") or ""

    if track.path.suffix.lower() == ".flac" and FLAC is not None and isinstance(audio, FLAC):
        audio["title"] = [tr_title]
        audio["artist"] = [artist_credit]
        audio["album"] = [album_title]
        if tr_no is not None:
            audio["tracknumber"] = [str(tr_no)]
        if date:
            audio["date"] = [date[:4]]
        if cover_bytes and Picture is not None:
            pic = Picture()
            pic.data = cover_bytes
            pic.type = 3
            pic.mime = "image/jpeg"
            audio.clear_pictures()
            audio.add_picture(pic)
        audio.save()
        return

    if track.path.suffix.lower() in {".m4a", ".aac", ".alac"} and MP4 is not None and isinstance(audio, MP4):
        audio["\xa9nam"] = [tr_title]
        audio["\xa9ART"] = [artist_credit]
        audio["\xa9alb"] = [album_title]
        if tr_no is not None:
            audio["trkn"] = [(tr_no, 0)]
        if date:
            audio["\xa9day"] = [date[:4]]
        if cover_bytes and MP4Cover is not None:
            audio["covr"] = [MP4Cover(cover_bytes, imageformat=MP4Cover.FORMAT_JPEG)]
        audio.save()
        return

    # MP3 / ID3 path
    if track.path.suffix.lower() == ".mp3" and ID3 is not None:
        if audio.tags is None:
            audio.add_tags()
        tags = audio.tags
        tags["TIT2"] = tags.get("TIT2") or type(tags.get("TIT2", None)) if False else None  # no-op placeholder
        from mutagen.id3 import TIT2, TALB, TPE1, TRCK, TDRC
        tags.add(TIT2(encoding=3, text=tr_title))
        tags.add(TALB(encoding=3, text=album_title))
        tags.add(TPE1(encoding=3, text=artist_credit))
        if tr_no is not None:
            tags.add(TRCK(encoding=3, text=str(tr_no)))
        if date:
            tags.add(TDRC(encoding=3, text=date[:4]))
        if cover_bytes and APIC is not None:
            tags.delall("APIC")
            tags.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=cover_bytes))
        audio.save()
        return


def process_album(album_dir: Path, artist_name: str, album_name: str, apply: bool) -> Dict[str, object]:
    local_tracks = load_local_album(album_dir, artist_name, album_name)
    if not local_tracks:
        return {"album_dir": str(album_dir), "skipped": True, "reason": "no_tracks"}

    match = choose_album_match(local_tracks, artist_name, album_name)
    if not match:
        return {
            "album_dir": str(album_dir),
            "artist": artist_name,
            "album": album_name,
            "skipped": True,
            "reason": "no_confident_match",
        }

    release_full = get_release_full(match.release_id)
    cover_bytes, cover_url = fetch_cover_art(match)

    result = {
        "album_dir": str(album_dir),
        "artist": artist_name,
        "album": album_name,
        "skipped": False,
        "matched_release": {
            "release_id": match.release_id,
            "release_group_id": match.release_group_id,
            "album_title": match.album_title,
            "artist_credit": match.artist_credit,
            "date": match.date,
            "status": match.status,
            "country": match.country,
            "track_total": match.track_total,
            "matched_tracks": match.matched_tracks,
            "matched_ratio": match.matched_ratio,
            "confidence": match.confidence,
        },
        "cover_art_found": bool(cover_bytes),
        "cover_art_url": cover_url,
        "tracks_updated": len(local_tracks) if apply else 0,
    }

    if apply:
        for t in local_tracks:
            apply_tags_and_art(t, release_full, cover_bytes)

    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Conservative metadata + album-art updater for Walkman albums.")
    parser.add_argument("--walkman-root", required=True, help='Path to Walkman MUSIC folder, e.g. /Volumes/MyMusic/MUSIC')
    parser.add_argument("--artist", help="Optional: only process one artist folder")
    parser.add_argument("--apply", action="store_true", help="Actually write tags and artwork. Default is dry-run.")
    parser.add_argument("--report-json", default="walkman_metadata_art_report.json", help="Where to write the JSON report")
    args = parser.parse_args(argv)

    walkman_root = Path(args.walkman_root).expanduser().resolve()
    if not walkman_root.exists():
        print(f"Walkman root not found: {walkman_root}", file=sys.stderr)
        return 2
    if certifi is None:
        print("certifi is required in the active venv for HTTPS.", file=sys.stderr)
        return 2

    album_dirs = scan_album_dirs(walkman_root)
    if args.artist:
        album_dirs = [x for x in album_dirs if x[1] == args.artist]

    results = []
    skipped = 0
    updated_albums = 0

    for album_dir, artist_name, album_name in album_dirs:
        res = process_album(album_dir, artist_name, album_name, args.apply)
        results.append(res)
        if res.get("skipped"):
            skipped += 1
        else:
            updated_albums += 1

    summary = {
        "albums_scanned": len(album_dirs),
        "albums_updated_or_ready": updated_albums,
        "albums_skipped": skipped,
        "apply_mode": args.apply,
        "results": results,
    }

    report_path = Path(args.report_json).expanduser().resolve()
    report_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Albums scanned: {summary['albums_scanned']}")
    print(f"Albums updated or ready: {summary['albums_updated_or_ready']}")
    print(f"Albums skipped: {summary['albums_skipped']}")
    print(f"Apply mode: {args.apply}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
