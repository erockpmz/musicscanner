from pathlib import Path
from .constants import DEFAULT_WALKMAN_MUSIC_CANDIDATES

def resolve_walkman_music_root(walkman_root: Path, music_subdir: str | None = None) -> Path:
    root = walkman_root.expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Walkman root does not exist: {root}")
    if music_subdir:
        p = root / music_subdir
        if p.exists() and p.is_dir():
            return p.resolve()
        raise SystemExit(f"Music subdir not found: {p}")
    for rel in DEFAULT_WALKMAN_MUSIC_CANDIDATES:
        p = root / rel
        if p.exists() and p.is_dir():
            return p.resolve()
    raise SystemExit(f"Could not find Walkman music folder under: {root}")
