import re
from dataclasses import dataclass
from pathlib import Path
from .constants import AUDIO_EXTENSIONS, RICHARD_LORSON
from .metadata import read_basic_tags, BasicTag

INVALID_CHARS = r'[<>:"/\\|?*\x00-\x1F]'

@dataclass
class PlannedMove:
    source: Path
    destination: Path
    reason: str

def sanitize_name(value: str, fallback: str) -> str:
    cleaned = re.sub(INVALID_CHARS, "_", value).strip().rstrip(".")
    return cleaned or fallback

def build_target_path(music_root: Path, src: Path, tag: BasicTag) -> Path:
    artist = sanitize_name(tag.artist, "Unknown Artist")
    album = sanitize_name(tag.album, "Unknown Album")
    title = sanitize_name(tag.title, src.stem)
    track = tag.track_number.split("/")[0].zfill(2) if tag.track_number else "00"
    return music_root / artist / album / f"{track} - {title}{src.suffix.lower()}"

def plan_renames(music_root: Path, skip_artists: set[str]) -> list[PlannedMove]:
    plans = []
    for p in sorted(music_root.rglob("*")):
        if not p.is_file() or p.suffix.lower() not in AUDIO_EXTENSIONS:
            continue
        tag = read_basic_tags(p)
        artist = tag.artist.strip().lower()
        if artist == RICHARD_LORSON:
            plans.append(PlannedMove(p, p, "skip_richard_lorson")); continue
        if artist and artist in skip_artists:
            plans.append(PlannedMove(p, p, "skip_artist")); continue
        dst = build_target_path(music_root, p, tag)
        if dst == p: 
            continue
        if dst.exists() and dst.resolve() != p.resolve():
            plans.append(PlannedMove(p, p, "conflict_target_exists")); continue
        plans.append(PlannedMove(p, dst, "rename"))
    return plans

def apply_plans(plans: list[PlannedMove], dry_run: bool = True) -> tuple[int, int]:
    moved, skipped = 0, 0
    for plan in plans:
        if plan.reason != "rename":
            skipped += 1
            continue
        if dry_run:
            continue
        plan.destination.parent.mkdir(parents=True, exist_ok=True)
        plan.source.rename(plan.destination)
        moved += 1
    return moved, skipped
