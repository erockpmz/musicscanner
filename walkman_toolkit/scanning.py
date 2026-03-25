from dataclasses import dataclass
from pathlib import Path
from .constants import AUDIO_EXTENSIONS

@dataclass(frozen=True)
class TrackRow:
    root: str
    path: str
    rel_path: str
    ext: str
    key: str

def scan_library_fast(root: Path) -> tuple[list[TrackRow], set[str]]:
    rows, keys = [], set()
    for p in sorted(root.rglob("*")):
        if not p.is_file() or p.suffix.lower() not in AUDIO_EXTENSIONS:
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        key = rel.rsplit(".", 1)[0].lower()
        row = TrackRow(str(root), str(p), rel, p.suffix.lower(), key)
        rows.append(row)
        keys.add(key)
    return rows, keys
