#!/usr/bin/env python3
"""Write Sony Walkman-compatible .m3u playlists directly from selected track paths.

Use this in playlist creation tools so no post-fix step is required.
"""

from __future__ import annotations

import argparse
from pathlib import Path


MUSIC_CANDIDATES = ("MUSIC", "Music", "music")


def resolve_music_root(walkman_root: Path, music_subdir: str | None = None) -> Path:
    if music_subdir:
        candidate = (walkman_root / music_subdir).resolve()
        if candidate.exists() and candidate.is_dir():
            return candidate
        raise SystemExit(f"Music subdir does not exist: {candidate}")

    for name in MUSIC_CANDIDATES:
        candidate = (walkman_root / name).resolve()
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise SystemExit(f"Could not find MUSIC folder under: {walkman_root}")


def write_walkman_playlist(
    walkman_root: Path,
    playlist_name: str,
    selected_tracks: list[Path],
    music_subdir: str | None = None,
    output_dir: Path | None = None,
    overwrite: bool = True,
) -> tuple[Path, int, int]:
    """Write playlist in Walkman-native format and return (file, kept, skipped)."""
    music_root = resolve_music_root(walkman_root, music_subdir)
    out_dir = output_dir.resolve() if output_dir else music_root
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{playlist_name}.m3u"

    if out_file.exists() and not overwrite:
        raise FileExistsError(f"Playlist already exists: {out_file}")

    cleaned: list[str] = []
    skipped = 0
    seen: set[str] = set()

    for raw in selected_tracks:
        track = raw.expanduser().resolve()
        if not track.exists() or not track.is_file():
            skipped += 1
            continue
        try:
            rel = track.relative_to(music_root)
        except ValueError:
            # Not under Walkman MUSIC; skip for compatibility.
            skipped += 1
            continue

        entry = str(rel).replace("\\", "/")
        if ".." in rel.parts:
            skipped += 1
            continue
        if entry in seen:
            continue
        seen.add(entry)
        cleaned.append(entry)

    lines = ["#EXTM3U", *cleaned]
    out_file.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return out_file, len(cleaned), skipped


def parse_track_list(path: Path) -> list[Path]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return [Path(line.strip()) for line in lines if line.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write Walkman-compatible .m3u from selected tracks")
    parser.add_argument("--walkman-root", required=True, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument("--playlist-name", required=True, help="Output playlist name without extension")
    parser.add_argument(
        "--tracks-file",
        required=True,
        help="Text file containing one absolute track path per line",
    )
    parser.add_argument("--music-subdir", default=None, help="Optional music subfolder under walkman root")
    parser.add_argument("--output-dir", default=None, help="Optional output folder (default: MUSIC root)")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not overwrite existing playlist")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    walkman_root = Path(args.walkman_root).expanduser().resolve()
    tracks_file = Path(args.tracks_file).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else None

    if not walkman_root.exists():
        raise SystemExit(f"Walkman root does not exist: {walkman_root}")
    if not tracks_file.exists() or not tracks_file.is_file():
        raise SystemExit(f"Tracks file does not exist: {tracks_file}")

    tracks = parse_track_list(tracks_file)
    out_file, kept, skipped = write_walkman_playlist(
        walkman_root=walkman_root,
        playlist_name=args.playlist_name,
        selected_tracks=tracks,
        music_subdir=args.music_subdir,
        output_dir=output_dir,
        overwrite=not args.no_overwrite,
    )

    print(f"Output playlist: {out_file}")
    print(f"Tracks kept: {kept}")
    print(f"Tracks skipped: {skipped}")


if __name__ == "__main__":
    main()
