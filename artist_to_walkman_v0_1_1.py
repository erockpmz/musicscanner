#!/usr/bin/env python3
"""
Generic artist organizer/sync tool for Sony Walkman workflows.

What it does
------------
1. Looks up an artist's studio albums and track lists online from MusicBrainz.
2. Matches those tracks against your local music_inventory.db catalog.
3. Scans the Walkman for existing files by that artist, including loose files in the
   artist root and files already in album folders.
4. Creates a proper folder structure on the Walkman:
      <WALKMAN_ROOT>/<Artist>/<Album>/<track files>
5. Moves matching existing Walkman files into the proper album folders when possible.
6. Copies missing tracks from your catalog to the Walkman.
7. Optionally removes loose root-level artist files from the Walkman, but only when a
   complete organized album exists after the move/copy plan.

Safety rules
------------
- No deletions happen unless you pass --cleanup-loose-root-files.
- Even with cleanup enabled, it only removes artist-root files for tracks that are
  duplicated by an organized file in the album folder after the plan is complete.
- It never deletes files for other artists.
- It writes a JSON report of everything it planned/did.

Defaults
--------
- Studio albums only.
- Official releases only.
- No live, no compilations, no EPs, no singles.
- No file tags are rewritten.
- No files are removed from your source library; only copies to the Walkman.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import ssl

try:
    import certifi
except Exception:
    certifi = None

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None


APP_NAME = "artist_to_walkman"
APP_VERSION = "0.1.1"

MUSICBRAINZ_BASE = "https://musicbrainz.org/ws/2"
SUPPORTED_AUDIO_EXTS = {".flac", ".m4a", ".mp3", ".wav", ".aiff", ".aac", ".alac"}

FORMAT_RANK = {
    "flac": 5,
    "alac": 4,
    "aac/m4a": 3,
    "mp3": 2,
    "wav": 1,
    "aiff": 1,
    "unknown": 0,
}

SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9._\-()'&, ]+")
SPACE_RE = re.compile(r"\s+")


@dataclass
class CatalogTrack:
    file_id: int
    path: str
    source_root: str
    rel_path: str
    format_name: str
    modified_ts: float
    file_size: int
    quick_hash: str
    metadata_score: int
    title: str
    artist: str
    album: str
    tracknumber: Optional[int]
    discnumber: Optional[int]
    duration: Optional[float]
    metadata: Dict[str, object]


@dataclass
class MBTrack:
    album_title: str
    album_sort_date: str
    track_title: str
    disc_number: int
    track_number: int
    track_count: int
    release_mbid: str
    release_group_mbid: str
    release_artist_credit: str
    track_artist_credit: str
    release_status: str
    release_group_primary_type: str
    release_group_secondary_types: List[str]


@dataclass
class WalkmanTrack:
    path: Path
    title: str
    artist: str
    album: str
    tracknumber: Optional[int]
    discnumber: Optional[int]
    ext: str
    rel_to_artist_root: str


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


def normalize_track_title(value: Optional[str]) -> str:
    value = normalize_text(value)
    value = re.sub(r"\blive\b", " ", value)
    value = re.sub(r"\bremaster(?:ed)?\b", " ", value)
    value = re.sub(r"\bversion\b", " ", value)
    value = SPACE_RE.sub(" ", value).strip()
    return value


def normalize_album_title(value: Optional[str]) -> str:
    value = normalize_text(value)
    value = re.sub(r"\bdeluxe\b", " ", value)
    value = re.sub(r"\bremaster(?:ed)?\b", " ", value)
    value = re.sub(r"\banniversary\b", " ", value)
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


def safe_name(value: str, limit: int = 120) -> str:
    value = value.strip().replace("/", "-").replace(":", " - ")
    value = SAFE_CHARS_RE.sub("", value)
    value = SPACE_RE.sub(" ", value).strip()
    if not value:
        value = "Unknown"
    return value[:limit].rstrip(" .")


def musicbrainz_get(entity: str, mbid: Optional[str] = None, **params) -> dict:
    if mbid:
        path = f"{MUSICBRAINZ_BASE}/{entity}/{mbid}"
    else:
        path = f"{MUSICBRAINZ_BASE}/{entity}"

    # MusicBrainz uses hyphenated browse parameters like release-group.
    normalized_params = {}
    for key, value in params.items():
        normalized_key = key.replace("_", "-")
        normalized_params[normalized_key] = value

    merged = {"fmt": "json"}
    merged.update(normalized_params)
    url = f"{path}?{urlencode(merged, doseq=True)}"

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


def mb_search_artists(name: str, limit: int = 10) -> List[dict]:
    data = musicbrainz_get("artist", query=f'artist:"{name}"', limit=limit)
    return data.get("artists", [])


def choose_best_artist(name: str, candidates: List[dict]) -> Optional[dict]:
    norm_name = normalize_text(name)
    if not candidates:
        return None

    def score(a: dict) -> Tuple[int, int, int]:
        cand_name = normalize_text(a.get("name"))
        exact = 1 if cand_name == norm_name else 0
        sort_exact = 1 if normalize_text(a.get("sort-name")) == norm_name else 0
        mb_score = int(a.get("score") or 0)
        return (exact, sort_exact, mb_score)

    return sorted(candidates, key=score, reverse=True)[0]


def browse_release_groups(artist_mbid: str, limit: int = 100) -> List[dict]:
    offset = 0
    out: List[dict] = []
    while True:
        data = musicbrainz_get(
            "release-group",
            artist=artist_mbid,
            limit=limit,
            offset=offset,
            inc="artist-credits",
        )
        groups = data.get("release-groups", [])
        out.extend(groups)
        if len(groups) < limit:
            break
        offset += limit
    return out


def select_candidate_release_group(
    rg: dict,
    include_eps: bool,
    include_compilations: bool,
    include_live: bool,
) -> bool:
    primary = (rg.get("primary-type") or "").casefold()
    secondaries = {(x or "").casefold() for x in rg.get("secondary-types", [])}

    allowed_primary = {"album"}
    if include_eps:
        allowed_primary.add("ep")
    if primary not in allowed_primary:
        return False

    if not include_compilations and "compilation" in secondaries:
        return False
    if not include_live and "live" in secondaries:
        return False
    banned = {"interview", "audiobook", "spokenword", "spoken word", "demo", "remix", "dj-mix", "mixtape/street"}
    if secondaries & banned:
        return False
    return True


def release_artist_credit_text(obj: dict) -> str:
    return "".join(ac.get("name", "") + ac.get("joinphrase", "") for ac in obj.get("artist-credit", []))


def browse_releases_for_group(release_group_mbid: str, limit: int = 100) -> List[dict]:
    offset = 0
    out: List[dict] = []
    while True:
        data = musicbrainz_get(
            "release",
            release_group=release_group_mbid,
            limit=limit,
            offset=offset,
            inc="artist-credits",
        )
        releases = data.get("releases", [])
        out.extend(releases)
        if len(releases) < limit:
            break
        offset += limit
    return out


def choose_best_release(releases: List[dict], allowed_statuses: Sequence[str]) -> Optional[dict]:
    if not releases:
        return None
    allowed = {x.casefold() for x in allowed_statuses}

    def score(r: dict) -> Tuple[int, int, int, str]:
        status = (r.get("status") or "").casefold()
        status_bonus = 10 if status in allowed else 0
        disambig_bonus = 0 if r.get("disambiguation") else 1
        country_bonus = 1 if r.get("country") else 0
        date = r.get("date") or "9999-99-99"
        return (status_bonus, disambig_bonus, country_bonus, date)

    releases_sorted = sorted(releases, key=score, reverse=True)
    for r in releases_sorted:
        if (r.get("status") or "").casefold() in allowed:
            return r
    return releases_sorted[0]


def fetch_release_with_tracks(release_mbid: str) -> dict:
    return musicbrainz_get("release", release_mbid, inc="recordings+artist-credits+media+release-groups")


def build_musicbrainz_track_plan(
    artist_names: Sequence[str],
    include_eps: bool = False,
    include_compilations: bool = False,
    include_live: bool = False,
    allowed_statuses: Sequence[str] = ("official",),
) -> Tuple[List[MBTrack], List[dict], List[dict]]:
    selected_artists: List[dict] = []
    release_groups_by_id: Dict[str, dict] = {}

    for name in artist_names:
        artist = choose_best_artist(name, mb_search_artists(name))
        if not artist:
            continue
        selected_artists.append({"name": name, "mbid": artist["id"], "matched_name": artist.get("name"), "score": artist.get("score")})
        for rg in browse_release_groups(artist["id"]):
            release_groups_by_id[rg["id"]] = rg

    selected_rgs = [
        rg for rg in release_groups_by_id.values()
        if select_candidate_release_group(rg, include_eps=include_eps, include_compilations=include_compilations, include_live=include_live)
    ]

    mb_tracks: List[MBTrack] = []
    selected_release_groups: List[dict] = []

    for rg in sorted(selected_rgs, key=lambda x: (x.get("first-release-date") or "9999-99-99", x.get("title") or "")):
        releases = browse_releases_for_group(rg["id"])
        best = choose_best_release(releases, allowed_statuses)
        if not best:
            continue
        full = fetch_release_with_tracks(best["id"])
        media = full.get("media", [])
        if not media:
            continue

        selected_release_groups.append(
            {
                "release_group_id": rg["id"],
                "title": rg.get("title"),
                "first_release_date": rg.get("first-release-date"),
                "primary_type": rg.get("primary-type"),
                "secondary_types": rg.get("secondary-types", []),
                "chosen_release_id": best["id"],
                "chosen_release_title": best.get("title"),
                "chosen_release_status": best.get("status"),
            }
        )

        for medium in media:
            disc_number = int(medium.get("position") or 1)
            tracks = medium.get("tracks", [])
            track_count = len(tracks)
            for idx, t in enumerate(tracks, start=1):
                rec = t.get("recording") or {}
                track_title = t.get("title") or rec.get("title") or ""
                mb_tracks.append(
                    MBTrack(
                        album_title=full.get("title") or rg.get("title") or "",
                        album_sort_date=full.get("date") or rg.get("first-release-date") or "",
                        track_title=track_title,
                        disc_number=disc_number,
                        track_number=parse_intish(t.get("number")) or idx,
                        track_count=track_count,
                        release_mbid=full["id"],
                        release_group_mbid=rg["id"],
                        release_artist_credit=release_artist_credit_text(full),
                        track_artist_credit=release_artist_credit_text(t) or release_artist_credit_text(rec),
                        release_status=(best.get("status") or ""),
                        release_group_primary_type=(rg.get("primary-type") or ""),
                        release_group_secondary_types=list(rg.get("secondary-types", [])),
                    )
                )

    return mb_tracks, selected_release_groups, selected_artists


def load_catalog(db_path: Path) -> List[CatalogTrack]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, path, source_root, rel_path, format_name, modified_ts, file_size, quick_hash,
               metadata_score, duration, metadata_json
        FROM files
        """
    ).fetchall()
    conn.close()

    out: List[CatalogTrack] = []
    for row in rows:
        metadata = json.loads(row["metadata_json"] or "{}")
        out.append(
            CatalogTrack(
                file_id=row["id"],
                path=row["path"],
                source_root=row["source_root"],
                rel_path=row["rel_path"],
                format_name=row["format_name"] or "unknown",
                modified_ts=float(row["modified_ts"] or 0),
                file_size=int(row["file_size"] or 0),
                quick_hash=row["quick_hash"] or "",
                metadata_score=int(row["metadata_score"] or 0),
                title=str(metadata.get("title") or ""),
                artist=str(metadata.get("artist") or metadata.get("albumartist") or ""),
                album=str(metadata.get("album") or ""),
                tracknumber=parse_intish(metadata.get("tracknumber")),
                discnumber=parse_intish(metadata.get("discnumber")),
                duration=float(row["duration"]) if row["duration"] is not None else None,
                metadata=metadata,
            )
        )
    return out


def artist_match(value: Optional[str], alias_names: Sequence[str]) -> bool:
    norm = normalize_text(value)
    alias_set = {normalize_text(x) for x in alias_names}
    return norm in alias_set


def build_catalog_indexes(
    catalog: List[CatalogTrack],
    alias_names: Sequence[str],
) -> Tuple[Dict[Tuple[str, str], List[CatalogTrack]], Dict[str, List[CatalogTrack]]]:
    by_album_track: Dict[Tuple[str, str], List[CatalogTrack]] = defaultdict(list)
    by_title: Dict[str, List[CatalogTrack]] = defaultdict(list)

    for tr in catalog:
        if not (
            artist_match(tr.artist, alias_names)
            or artist_match(tr.metadata.get("albumartist"), alias_names)
        ):
            continue
        norm_title = normalize_track_title(tr.title)
        norm_album = normalize_album_title(tr.album)
        by_title[norm_title].append(tr)
        by_album_track[(norm_album, norm_title)].append(tr)

    return by_album_track, by_title


def candidate_sort_key(track: CatalogTrack) -> Tuple[int, int, float, int]:
    return (
        FORMAT_RANK.get(track.format_name, 0),
        int(track.metadata_score or 0),
        float(track.modified_ts or 0),
        int(track.file_size or 0),
    )


def choose_best_catalog_match(
    mb_track: MBTrack,
    by_album_track: Dict[Tuple[str, str], List[CatalogTrack]],
    by_title: Dict[str, List[CatalogTrack]],
) -> Optional[CatalogTrack]:
    norm_title = normalize_track_title(mb_track.track_title)
    norm_album = normalize_album_title(mb_track.album_title)

    candidates = list(by_album_track.get((norm_album, norm_title), []))
    if not candidates:
        candidates = list(by_title.get(norm_title, []))

    if mb_track.disc_number:
        disc_filtered = [c for c in candidates if c.discnumber in (None, mb_track.disc_number)]
        if disc_filtered:
            candidates = disc_filtered

    if mb_track.track_number:
        tr_filtered = [c for c in candidates if c.tracknumber in (None, mb_track.track_number)]
        if tr_filtered:
            candidates = tr_filtered

    if not candidates:
        return None

    candidates.sort(key=candidate_sort_key, reverse=True)
    return candidates[0]


def parse_filename_fallback(path: Path) -> Dict[str, object]:
    stem = path.stem
    title = stem
    tracknumber = None
    m = re.match(r"^\s*(\d{1,3})\s*[-._ ]+\s*(.+?)\s*$", stem)
    if m:
        tracknumber = int(m.group(1))
        title = m.group(2)
    album = path.parent.name if path.parent else ""
    artist = path.parent.parent.name if path.parent and path.parent.parent else ""
    return {
        "title": title,
        "artist": artist,
        "album": album,
        "tracknumber": tracknumber,
        "discnumber": None,
    }


def read_tags_from_file(path: Path) -> Dict[str, object]:
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

    out = {
        "title": first_value(["title", "\xa9nam", "TIT2"]) or fallback["title"],
        "artist": first_value(["artist", "albumartist", "\xa9ART", "TPE1", "TPE2"]) or fallback["artist"],
        "album": first_value(["album", "\xa9alb", "TALB"]) or fallback["album"],
        "tracknumber": parse_intish(first_value(["tracknumber", "trkn", "TRCK"])) or fallback["tracknumber"],
        "discnumber": parse_intish(first_value(["discnumber", "disk", "TPOS"])) or fallback["discnumber"],
    }
    return out


def scan_walkman_artist_files(walkman_root: Path, artist_dir_name: str, alias_names: Sequence[str]) -> List[WalkmanTrack]:
    artist_root = walkman_root / artist_dir_name
    out: List[WalkmanTrack] = []
    if not artist_root.exists():
        return out

    for dirpath, _, filenames in os.walk(artist_root):
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.suffix.lower() not in SUPPORTED_AUDIO_EXTS:
                continue
            tags = read_tags_from_file(path)
            title = str(tags.get("title") or "")
            artist = str(tags.get("artist") or artist_dir_name)
            album = str(tags.get("album") or "")
            rel = path.relative_to(artist_root).as_posix()
            if not (artist_match(artist, alias_names) or normalize_text(artist_dir_name) in normalize_text(rel)):
                continue
            out.append(
                WalkmanTrack(
                    path=path,
                    title=title,
                    artist=artist,
                    album=album,
                    tracknumber=parse_intish(tags.get("tracknumber")),
                    discnumber=parse_intish(tags.get("discnumber")),
                    ext=path.suffix,
                    rel_to_artist_root=rel,
                )
            )
    return out


def choose_best_walkman_match(mb_track: MBTrack, walkman_tracks: List[WalkmanTrack]) -> Optional[WalkmanTrack]:
    norm_title = normalize_track_title(mb_track.track_title)
    norm_album = normalize_album_title(mb_track.album_title)
    candidates = [
        t for t in walkman_tracks
        if normalize_track_title(t.title) == norm_title
    ]
    album_candidates = [t for t in candidates if normalize_album_title(t.album) == norm_album]
    if album_candidates:
        candidates = album_candidates
    if mb_track.track_number:
        tr_filtered = [t for t in candidates if t.tracknumber in (None, mb_track.track_number)]
        if tr_filtered:
            candidates = tr_filtered
    if mb_track.disc_number:
        disc_filtered = [t for t in candidates if t.discnumber in (None, mb_track.disc_number)]
        if disc_filtered:
            candidates = disc_filtered
    return candidates[0] if candidates else None


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_dest_path(walkman_root: Path, artist_dir_name: str, mb_track: MBTrack, ext: str) -> Path:
    artist_dir = safe_name(artist_dir_name)
    album_dir = safe_name(mb_track.album_title)
    disc_prefix = f"{mb_track.disc_number:02d}-" if mb_track.disc_number and mb_track.disc_number > 1 else ""
    filename = f"{disc_prefix}{mb_track.track_number:02d} {safe_name(mb_track.track_title)}{ext}"
    return walkman_root / artist_dir / album_dir / filename


def relative_playlist_path(playlist_path: Path, track_path: Path) -> str:
    return os.path.relpath(track_path, start=playlist_path.parent).replace(os.sep, "/")


def write_playlist(path: Path, track_paths: List[Path]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for p in track_paths:
            f.write(relative_playlist_path(path, p) + "\n")


def bytes_to_gb(value: int) -> float:
    return round(value / (1024 ** 3), 2)


def free_space_bytes(path: Path) -> int:
    usage = shutil.disk_usage(str(path))
    return int(usage.free)


def sync_artist(
    db_path: Path,
    walkman_root: Path,
    artist_name: str,
    artist_dir_name: Optional[str],
    artist_aliases: Sequence[str],
    dry_run: bool = False,
    cleanup_loose_root_files: bool = False,
    write_playlists: bool = True,
) -> dict:
    lookup_names = [artist_name] + list(artist_aliases)
    artist_dir_name = artist_dir_name or artist_name
    alias_names = [artist_name] + list(artist_aliases)

    catalog = load_catalog(db_path)
    by_album_track, by_title = build_catalog_indexes(catalog, alias_names)

    mb_tracks, selected_release_groups, selected_artists = build_musicbrainz_track_plan(
        lookup_names,
        include_eps=False,
        include_compilations=False,
        include_live=False,
    )

    walkman_tracks = scan_walkman_artist_files(walkman_root, artist_dir_name, alias_names)

    report = {
        "artist_name": artist_name,
        "artist_directory": artist_dir_name,
        "selected_artists": selected_artists,
        "selected_release_groups": selected_release_groups,
        "matched_from_catalog": [],
        "matched_existing_on_walkman": [],
        "copied": [],
        "moved_existing": [],
        "removed_loose_root_duplicates": [],
        "unmatched_musicbrainz_tracks": [],
        "storage": {},
        "playlists": [],
    }

    album_to_paths: Dict[str, List[Tuple[Tuple[int, int, str], Path]]] = defaultdict(list)
    bytes_to_copy = 0

    for mb_track in mb_tracks:
        existing = choose_best_walkman_match(mb_track, walkman_tracks)
        if existing:
            dest_from_existing = build_dest_path(walkman_root, artist_dir_name, mb_track, existing.ext)
            report["matched_existing_on_walkman"].append(
                {
                    "album": mb_track.album_title,
                    "track": mb_track.track_title,
                    "existing_path": str(existing.path),
                    "dest_path": str(dest_from_existing),
                }
            )
            sort_key = (mb_track.disc_number, mb_track.track_number, normalize_track_title(mb_track.track_title))
            album_to_paths[mb_track.album_title].append((sort_key, dest_from_existing))
            continue

        chosen = choose_best_catalog_match(mb_track, by_album_track, by_title)
        if not chosen:
            report["unmatched_musicbrainz_tracks"].append(
                {
                    "album": mb_track.album_title,
                    "track": mb_track.track_title,
                    "disc": mb_track.disc_number,
                    "track_number": mb_track.track_number,
                }
            )
            continue

        src = Path(chosen.path)
        dst = build_dest_path(walkman_root, artist_dir_name, mb_track, src.suffix)
        report["matched_from_catalog"].append(
            {
                "album": mb_track.album_title,
                "track": mb_track.track_title,
                "source_path": chosen.path,
                "dest_path": str(dst),
                "bytes": chosen.file_size,
                "format": chosen.format_name,
            }
        )
        bytes_to_copy += int(chosen.file_size or 0)
        sort_key = (mb_track.disc_number, mb_track.track_number, normalize_track_title(mb_track.track_title))
        album_to_paths[mb_track.album_title].append((sort_key, dst))

    free_bytes = free_space_bytes(walkman_root)
    report["storage"] = {
        "bytes_to_copy": bytes_to_copy,
        "gb_to_copy": bytes_to_gb(bytes_to_copy),
        "walkman_free_bytes": free_bytes,
        "walkman_free_gb": bytes_to_gb(free_bytes),
        "fits": free_bytes >= bytes_to_copy,
    }

    for item in report["matched_existing_on_walkman"]:
        src = Path(item["existing_path"])
        dst = Path(item["dest_path"])
        if src == dst:
            continue
        if not dry_run:
            ensure_parent(dst)
            if not dst.exists():
                shutil.move(str(src), str(dst))
        report["moved_existing"].append({"from": str(src), "to": str(dst)})

    for item in report["matched_from_catalog"]:
        src = Path(item["source_path"])
        dst = Path(item["dest_path"])
        if not dry_run:
            ensure_parent(dst)
            if not dst.exists():
                shutil.copy2(src, dst)
        report["copied"].append({"from": str(src), "to": str(dst), "bytes": item["bytes"]})

    album_expected_counts: Dict[str, int] = {}
    for mb_track in mb_tracks:
        album_expected_counts.setdefault(mb_track.album_title, mb_track.track_count)

    organized_album_files: Dict[str, set] = defaultdict(set)
    for album, items in album_to_paths.items():
        for _, path in items:
            organized_album_files[album].add((normalize_track_title(path.stem), path))

    complete_albums = {
        album for album, expected in album_expected_counts.items()
        if len(organized_album_files.get(album, set())) >= expected
    }

    artist_root = walkman_root / artist_dir_name
    for w in walkman_tracks:
        is_loose_root = w.path.parent == artist_root
        if not is_loose_root:
            continue
        title_norm = normalize_track_title(w.title)
        album_norm = normalize_album_title(w.album)
        for album in complete_albums:
            if album_norm and album_norm != normalize_album_title(album):
                continue
            for organized_title_norm, organized_path in organized_album_files.get(album, set()):
                if organized_title_norm == title_norm and organized_path != w.path:
                    if cleanup_loose_root_files:
                        if not dry_run and w.path.exists():
                            w.path.unlink()
                        report["removed_loose_root_duplicates"].append(
                            {
                                "removed": str(w.path),
                                "replacement": str(organized_path),
                                "album": album,
                            }
                        )
                    break

    if write_playlists:
        playlist_root = artist_root / "Playlists"
        for album, items in sorted(album_to_paths.items(), key=lambda kv: normalize_album_title(kv[0])):
            items.sort(key=lambda x: x[0])
            playlist_path = playlist_root / f"{safe_name(album)}.m3u"
            track_paths = [p for _, p in items]
            if not dry_run:
                write_playlist(playlist_path, track_paths)
            report["playlists"].append(str(playlist_path))

    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Organize an artist on a Sony Walkman using MusicBrainz track lists and your local inventory DB.")
    parser.add_argument("--db", required=True, help="Path to music_inventory.db")
    parser.add_argument("--walkman-root", required=True, help="Path to Walkman MUSIC folder, e.g. /Volumes/MyMusic/MUSIC")
    parser.add_argument("--artist", required=True, help="Primary artist name, e.g. 'Bruce Springsteen'")
    parser.add_argument("--artist-dir-name", help="Folder name to use on the Walkman. Default: same as --artist")
    parser.add_argument("--artist-alias", action="append", default=[], help="Additional artist names to include for lookup and matching. Repeat as needed.")
    parser.add_argument("--dry-run", action="store_true", help="Do not move/copy/delete anything; just produce report")
    parser.add_argument("--cleanup-loose-root-files", action="store_true", help="Remove loose artist-root files only when a complete organized album exists")
    parser.add_argument("--no-playlists", action="store_true", help="Do not write per-album playlists")
    parser.add_argument("--report-json", default="artist_sync_report.json", help="Where to write JSON report")
    args = parser.parse_args(argv)

    db_path = Path(args.db).expanduser().resolve()
    walkman_root = Path(args.walkman_root).expanduser().resolve()

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 2
    if not walkman_root.exists():
        print(f"Walkman root not found: {walkman_root}", file=sys.stderr)
        return 2

    report = sync_artist(
        db_path=db_path,
        walkman_root=walkman_root,
        artist_name=args.artist,
        artist_dir_name=args.artist_dir_name,
        artist_aliases=args.artist_alias,
        dry_run=args.dry_run,
        cleanup_loose_root_files=args.cleanup_loose_root_files,
        write_playlists=not args.no_playlists,
    )

    report_path = Path(args.report_json).expanduser().resolve()
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Artist: {report['artist_name']}")
    print(f"Matched from catalog: {len(report['matched_from_catalog'])}")
    print(f"Matched existing on Walkman: {len(report['matched_existing_on_walkman'])}")
    print(f"Copied: {len(report['copied'])}")
    print(f"Moved existing: {len(report['moved_existing'])}")
    print(f"Removed loose root duplicates: {len(report['removed_loose_root_duplicates'])}")
    print(f"Unmatched MusicBrainz tracks: {len(report['unmatched_musicbrainz_tracks'])}")
    storage = report["storage"]
    print(f"Estimated copy size: {storage['gb_to_copy']} GB")
    print(f"Walkman free space: {storage['walkman_free_gb']} GB")
    print(f"Fits: {storage['fits']}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
