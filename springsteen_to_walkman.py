#!/usr/bin/env python3
"""
Springsteen sync tool for Sony Walkman NW-A55 workflows.

What it does
------------
- Looks up Bruce Springsteen album/release data online from MusicBrainz.
- Builds album + track lists for Bruce Springsteen, including Bruce Springsteen &
  The E Street Band / Bruce Springsteen and The E Street Band credits.
- Matches those tracks against your existing music_inventory.db catalog.
- Copies missing matched tracks onto the Walkman.
- Writes per-album .m3u playlists plus one master playlist.

This tool is conservative:
- It never deletes from the Walkman.
- It only copies tracks that it can confidently match.
- It keeps a JSON report of matched / unmatched / skipped tracks.

Default behavior
----------------
Includes official release groups of these types:
- album
- ep
- live

Excludes by default:
- bootlegs
- singles
- broad "compilation" release groups

You can override with CLI flags.
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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

APP_NAME = "springsteen_to_walkman"
APP_VERSION = "0.1.0"

MUSICBRAINZ_BASE = "https://musicbrainz.org/ws/2"

BRUCE_MBID = "70248960-cb53-4ea4-943a-edb18f7d336f"
ESTREET_MBID = "d6652e7b-33fe-49ef-8336-4c863b4f996f"

DEFAULT_RELEASE_GROUP_TYPES = {"album", "ep", "live"}
DEFAULT_ALLOWED_STATUSES = {"official"}

DEFAULT_ARTIST_PATTERNS = [
    "bruce springsteen",
    "bruce springsteen & the e street band",
    "bruce springsteen and the e street band",
]

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


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.casefold().strip()
    value = value.replace("&", " and ")
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = value.replace("’", "'").replace("`", "'")
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
    merged = {"fmt": "json"}
    merged.update(params)
    url = f"{path}?{urlencode(merged, doseq=True)}"
    req = Request(
        url,
        headers={
            "User-Agent": f"{APP_NAME}/{APP_VERSION} (personal local script)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    time.sleep(1.05)  # stay polite with MusicBrainz
    return data


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
    allowed_primary_types: Sequence[str],
    include_compilations: bool,
) -> bool:
    primary = (rg.get("primary-type") or "").casefold()
    secondaries = {(x or "").casefold() for x in rg.get("secondary-types", [])}
    if primary not in {x.casefold() for x in allowed_primary_types}:
        return False
    if not include_compilations and "compilation" in secondaries:
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
        disambig = 0 if r.get("disambiguation") else 1
        packaging = (r.get("packaging") or "").casefold()
        fmt_bonus = 1 if packaging in {"none", ""} else 0
        status_bonus = 10 if status in allowed else 0
        date = r.get("date") or "9999-99-99"
        return (status_bonus, disambig, fmt_bonus, date)

        # earlier official / cleaner release wins

    releases_sorted = sorted(releases, key=score)
    # Prefer an allowed status first if any exist
    for r in releases_sorted:
        if (r.get("status") or "").casefold() in allowed:
            return r
    return releases_sorted[0]


def fetch_release_with_tracks(release_mbid: str) -> dict:
    return musicbrainz_get("release", release_mbid, inc="recordings+artist-credits+media+release-groups")


def build_musicbrainz_track_plan(
    include_compilations: bool = False,
    include_live: bool = True,
    allowed_statuses: Sequence[str] = tuple(DEFAULT_ALLOWED_STATUSES),
) -> Tuple[List[MBTrack], List[dict]]:
    allowed_types = {"album", "ep"}
    if include_live:
        allowed_types.add("live")

    release_groups_by_id: Dict[str, dict] = {}
    for mbid in (BRUCE_MBID, ESTREET_MBID):
        for rg in browse_release_groups(mbid):
            release_groups_by_id[rg["id"]] = rg

    selected_rgs = [
        rg for rg in release_groups_by_id.values()
        if select_candidate_release_group(rg, sorted(allowed_types), include_compilations)
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

    return mb_tracks, selected_release_groups


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


def is_springsteen_artist(value: Optional[str]) -> bool:
    norm = normalize_text(value)
    return norm in {normalize_text(x) for x in DEFAULT_ARTIST_PATTERNS}


def build_catalog_indexes(catalog: List[CatalogTrack], walkman_root: str) -> Tuple[Dict[Tuple[str, str], List[CatalogTrack]], Dict[str, List[CatalogTrack]], set]:
    by_album_track: Dict[Tuple[str, str], List[CatalogTrack]] = defaultdict(list)
    by_title: Dict[str, List[CatalogTrack]] = defaultdict(list)
    walkman_hashes: set = set()

    for tr in catalog:
        norm_title = normalize_track_title(tr.title)
        norm_album = normalize_album_title(tr.album)
        by_title[norm_title].append(tr)
        by_album_track[(norm_album, norm_title)].append(tr)
        if tr.source_root == walkman_root and tr.quick_hash:
            walkman_hashes.add(tr.quick_hash)

    return by_album_track, by_title, walkman_hashes


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

    springsteen_candidates = [
        c for c in candidates if is_springsteen_artist(c.artist) or is_springsteen_artist(c.metadata.get("albumartist"))
    ]
    if springsteen_candidates:
        candidates = springsteen_candidates

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


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_dest_path(walkman_root: Path, mb_track: MBTrack, source_track: CatalogTrack) -> Path:
    artist_dir = safe_name("Bruce Springsteen")
    album_dir = safe_name(mb_track.album_title)
    ext = Path(source_track.path).suffix
    disc_prefix = f"{mb_track.disc_number:02d}-" if mb_track.disc_number and mb_track.disc_number > 1 else ""
    filename = f"{disc_prefix}{mb_track.track_number:02d} {safe_name(mb_track.track_title)}{ext}"
    return walkman_root / artist_dir / album_dir / filename


def relative_playlist_path(playlist_path: Path, track_path: Path) -> str:
    return os.path.relpath(track_path, start=playlist_path.parent).replace(os.sep, "/")


def copy_if_needed(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    if dst.exists():
        return
    shutil.copy2(src, dst)


def write_playlist(path: Path, track_paths: List[Path]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for p in track_paths:
            f.write(relative_playlist_path(path, p) + "\n")


def sync_springsteen(
    db_path: Path,
    walkman_root: Path,
    include_compilations: bool = False,
    include_live: bool = True,
    dry_run: bool = False,
    playlist_subdir: str = "Playlists/Bruce Springsteen",
) -> dict:
    catalog = load_catalog(db_path)
    by_album_track, by_title, walkman_hashes = build_catalog_indexes(catalog, str(walkman_root))

    mb_tracks, selected_release_groups = build_musicbrainz_track_plan(
        include_compilations=include_compilations,
        include_live=include_live,
    )

    report = {
        "selected_release_groups": selected_release_groups,
        "matched": [],
        "copied": [],
        "skipped_already_on_walkman": [],
        "unmatched": [],
        "playlists": [],
    }

    album_to_paths: Dict[str, List[Tuple[Tuple[int, int, str], Path]]] = defaultdict(list)
    master_paths: List[Path] = []

    for mb_track in mb_tracks:
        chosen = choose_best_catalog_match(mb_track, by_album_track, by_title)
        if not chosen:
            report["unmatched"].append(
                {
                    "album": mb_track.album_title,
                    "track": mb_track.track_title,
                    "disc": mb_track.disc_number,
                    "track_number": mb_track.track_number,
                }
            )
            continue

        src = Path(chosen.path)
        dst = build_dest_path(walkman_root, mb_track, chosen)

        report["matched"].append(
            {
                "album": mb_track.album_title,
                "track": mb_track.track_title,
                "source_path": chosen.path,
                "dest_path": str(dst),
                "format": chosen.format_name,
                "hash": chosen.quick_hash,
            }
        )

        if chosen.quick_hash and chosen.quick_hash in walkman_hashes:
            report["skipped_already_on_walkman"].append(
                {
                    "album": mb_track.album_title,
                    "track": mb_track.track_title,
                    "source_path": chosen.path,
                    "dest_path": str(dst),
                    "reason": "same_quick_hash_already_on_walkman",
                }
            )
        else:
            if not dry_run:
                copy_if_needed(src, dst)
            report["copied"].append(
                {
                    "album": mb_track.album_title,
                    "track": mb_track.track_title,
                    "source_path": chosen.path,
                    "dest_path": str(dst),
                }
            )

        sort_key = (mb_track.disc_number, mb_track.track_number, normalize_track_title(mb_track.track_title))
        album_to_paths[mb_track.album_title].append((sort_key, dst))
        master_paths.append(dst)

    playlist_root = walkman_root / playlist_subdir
    for album, items in sorted(album_to_paths.items(), key=lambda kv: normalize_album_title(kv[0])):
        items.sort(key=lambda x: x[0])
        playlist_path = playlist_root / f"{safe_name(album)}.m3u"
        track_paths = [p for _, p in items]
        if not dry_run:
            write_playlist(playlist_path, track_paths)
        report["playlists"].append(str(playlist_path))

    master_playlist = playlist_root / "Bruce Springsteen Complete.m3u"
    master_paths_sorted = sorted(
        set(master_paths),
        key=lambda p: (
            normalize_album_title(p.parent.name),
            p.name.casefold(),
        ),
    )
    if not dry_run:
        write_playlist(master_playlist, master_paths_sorted)
    report["playlists"].append(str(master_playlist))

    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sync Bruce Springsteen albums/tracks to Sony Walkman using MusicBrainz lookup and local catalog matching.")
    parser.add_argument("--db", required=True, help="Path to music_inventory.db")
    parser.add_argument("--walkman-root", required=True, help="Path to Walkman MUSIC folder, e.g. /Volumes/MyMusic/MUSIC")
    parser.add_argument("--include-compilations", action="store_true", help="Include compilation release groups")
    parser.add_argument("--exclude-live", action="store_true", help="Exclude live release groups")
    parser.add_argument("--dry-run", action="store_true", help="Do not copy or write playlists; just produce report")
    parser.add_argument("--report-json", default="springsteen_sync_report.json", help="Where to write JSON report")
    args = parser.parse_args(argv)

    db_path = Path(args.db).expanduser().resolve()
    walkman_root = Path(args.walkman_root).expanduser().resolve()

    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        return 2
    if not walkman_root.exists():
        print(f"Walkman root not found: {walkman_root}", file=sys.stderr)
        return 2

    report = sync_springsteen(
        db_path=db_path,
        walkman_root=walkman_root,
        include_compilations=args.include_compilations,
        include_live=not args.exclude_live,
        dry_run=args.dry_run,
    )

    report_path = Path(args.report_json).expanduser().resolve()
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Matched: {len(report['matched'])}")
    print(f"Copied: {len(report['copied'])}")
    print(f"Skipped already on Walkman: {len(report['skipped_already_on_walkman'])}")
    print(f"Unmatched: {len(report['unmatched'])}")
    print(f"Playlists: {len(report['playlists'])}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
