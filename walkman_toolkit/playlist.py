import os
from dataclasses import dataclass
from pathlib import Path
from .constants import AUDIO_EXTENSIONS

@dataclass
class PlaylistValidationResult:
    playlist_exists: bool
    missing_files: list[str]
    bad_paths: list[str]

def collect_tracks(root: Path) -> list[Path]:
    return [p for p in sorted(root.rglob("*")) if p.is_file() and p.suffix.lower() in AUDIO_EXTENSIONS]

def write_m3u(playlist_path: Path, track_paths: list[Path], relative: bool = True) -> Path:
    playlist_path.parent.mkdir(parents=True, exist_ok=True)
    base = playlist_path.parent.resolve()
    lines = ["#EXTM3U"]
    for track in track_paths:
        t = track.resolve()
        entry = os.path.relpath(str(t), str(base)) if relative else str(t)
        lines.append(entry.replace("\\", "/"))
    playlist_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return playlist_path

def validate_m3u(playlist_path: Path) -> PlaylistValidationResult:
    if not playlist_path.exists():
        return PlaylistValidationResult(False, [], [])
    missing, bad = [], []
    for line in playlist_path.read_text(encoding="utf-8", errors="replace").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        if "\\" in item:
            bad.append(item)
        if not (playlist_path.parent / item).resolve().exists():
            missing.append(item)
    return PlaylistValidationResult(True, missing, bad)
