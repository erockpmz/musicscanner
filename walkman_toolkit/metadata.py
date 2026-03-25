from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass
class BasicTag:
    artist: str = ""
    album: str = ""
    title: str = ""
    track_number: str = ""

def _first(v: Any) -> str:
    if isinstance(v, list):
        return str(v[0]) if v else ""
    return str(v) if v is not None else ""

def read_basic_tags(path: Path) -> BasicTag:
    from mutagen import File as MutagenFile
    data = MutagenFile(path, easy=True)
    if data is None:
        return BasicTag()
    return BasicTag(
        artist=_first(data.get("artist", [""])).strip(),
        album=_first(data.get("album", [""])).strip(),
        title=_first(data.get("title", [""])).strip(),
        track_number=_first(data.get("tracknumber", [""])).strip(),
    )
