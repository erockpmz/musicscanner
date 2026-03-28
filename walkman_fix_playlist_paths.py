#!/usr/bin/env python3
"""Rewrite playlists into Sony Walkman-friendly .m3u format.

Why this helps:
- Many Walkman models ignore playlist entries containing parent traversal (`..`).
- This tool rewrites entries as paths relative to the Walkman's MUSIC folder.
- Output defaults to the MUSIC folder to maximize compatibility.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def resolve_music_root(walkman_root: Path, music_subdir: str | None) -> Path:
    if music_subdir:
        candidate = walkman_root / music_subdir
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()
        raise SystemExit(f"Music subdir does not exist: {candidate}")

    for name in ("MUSIC", "Music", "music"):
        candidate = walkman_root / name
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()

    raise SystemExit(f"Could not find MUSIC folder under: {walkman_root}")


def parse_playlist_entries(playlist_file: Path) -> list[str]:
    lines = playlist_file.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]


def to_abs_path(entry: str, playlist_file: Path) -> Path:
    p = Path(entry)
    if p.is_absolute():
        return p.resolve()
    return (playlist_file.parent / p).resolve()


def rewrite_playlist(
    playlist_file: Path,
    music_root: Path,
    output_dir: Path,
    overwrite: bool,
) -> tuple[Path, int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / f"{playlist_file.stem}.m3u"
    if out_file.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {out_file} (use --overwrite)")

    kept: list[str] = []
    skipped = 0

    for raw_entry in parse_playlist_entries(playlist_file):
        abs_track = to_abs_path(raw_entry, playlist_file)
        if not abs_track.exists():
            skipped += 1
            continue
        try:
            rel = abs_track.relative_to(music_root)
        except ValueError:
            # Track is not under this Walkman MUSIC root; skip for compatibility.
            skipped += 1
            continue
        kept.append(str(rel).replace("\\", "/"))

    lines = ["#EXTM3U", *kept]
    out_file.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return out_file, len(kept), skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fix playlist paths for Sony Walkman compatibility")
    parser.add_argument("--walkman-root", required=True, help="Mounted Walkman root, e.g. /Volumes/MyMusic")
    parser.add_argument(
        "--playlist-file",
        required=True,
        help="Input playlist file (.m3u or .m3u8) to rewrite",
    )
    parser.add_argument(
        "--music-subdir",
        default=None,
        help="Optional music subfolder under walkman root (default: auto-detect MUSIC)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output folder (default: WALKMAN_ROOT/MUSIC)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output .m3u")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    walkman_root = Path(args.walkman_root).expanduser().resolve()
    playlist_file = Path(args.playlist_file).expanduser().resolve()

    if not walkman_root.exists():
        raise SystemExit(f"Walkman root does not exist: {walkman_root}")
    if not playlist_file.exists() or not playlist_file.is_file():
        raise SystemExit(f"Playlist file does not exist: {playlist_file}")

    music_root = resolve_music_root(walkman_root, args.music_subdir)
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else music_root

    out_file, kept_count, skipped_count = rewrite_playlist(
        playlist_file=playlist_file,
        music_root=music_root,
        output_dir=output_dir,
        overwrite=args.overwrite,
    )

    print(f"Input playlist: {playlist_file}")
    print(f"Walkman music root: {music_root}")
    print(f"Output playlist: {out_file}")
    print(f"Tracks kept: {kept_count}")
    print(f"Tracks skipped (missing or outside MUSIC): {skipped_count}")


if __name__ == "__main__":
    main()
