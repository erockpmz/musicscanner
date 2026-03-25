#!/usr/bin/env python3
"""
Walkman Playlist Builder

Local web app to browse tracks already stored on a mounted Sony Walkman, search/select them, and save playlists as .m3u8 files back to that same device.

How it works
- Auto-detects a mounted Sony Walkman on macOS, or uses a path you provide
- Scans the Walkman's own music files
- Reads metadata when available (artist / album / title / track)
- Hosts a local web page
- Lets you search and add tracks to a playlist
- Saves the playlist as UTF-8 .m3u8 back onto the Walkman using relative paths

Good defaults for your setup
- The Walkman itself is both the source and the destination
- Playlists are saved into a Playlists folder on the Walkman

Requirements
    pip install flask mutagen

Run example
    python3 walkman_playlist_builder.py \
      --walkman-root '/Volumes/WALKMAN' \
      --host 127.0.0.1 \
      --port 5050

Then open:
    http://127.0.0.1:5050

Notes
- The Sony Walkman usually handles .m3u8 well, but exact behavior can vary by model.
- Relative paths are safest when the playlist lives on the Walkman alongside its music tree.
- This script only scans files and writes playlist files; it does not modify audio files.
"""

from __future__ import annotations

import argparse
import os
import re
import threading
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request, Response
from mutagen import File as MutagenFile

AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".alac"
}

DEFAULT_PLAYLIST_DIR_CANDIDATES = [
    "MUSIC/Playlists",
    "MUSIC/Playlist",
    "Playlists",
    "PLAYLISTS",
]

DEFAULT_MUSIC_DIR_CANDIDATES = [
    "MUSIC",
    "MUSIC/Tracks",
    "MUSIC/Music",
]


@dataclass
class Track:
    id: int
    path: str
    rel_path: str
    filename: str
    artist: str
    album: str
    title: str
    track_number: str


class TrackLibrary:
    def __init__(self, music_root: Path) -> None:
        self.music_root = music_root.resolve()
        self._tracks: list[Track] = []
        self._by_id: dict[int, Track] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _clean_tag(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return str(value[0]).strip() if value else ""
        return str(value).strip()

    @staticmethod
    def _first_present(tags: dict, *keys: str) -> str:
        for key in keys:
            if key in tags:
                value = TrackLibrary._clean_tag(tags.get(key))
                if value:
                    return value
        return ""

    def _read_metadata(self, file_path: Path) -> tuple[str, str, str, str]:
        artist = ""
        album = ""
        title = ""
        track_number = ""

        try:
            audio = MutagenFile(str(file_path), easy=True)
            if audio and getattr(audio, "tags", None):
                tags = dict(audio.tags)
                artist = self._first_present(tags, "artist", "albumartist")
                album = self._first_present(tags, "album")
                title = self._first_present(tags, "title")
                track_number = self._first_present(tags, "tracknumber")
        except Exception:
            pass

        if not title:
            title = file_path.stem
        if not artist:
            artist = "Unknown Artist"
        if not album:
            album = "Unknown Album"

        return artist, album, title, track_number

    def scan(self) -> int:
        found: list[Track] = []
        idx = 1

        for path in sorted(self.music_root.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix.lower() not in AUDIO_EXTENSIONS:
                continue

            rel_path = str(path.relative_to(self.music_root))
            artist, album, title, track_number = self._read_metadata(path)
            found.append(
                Track(
                    id=idx,
                    path=str(path),
                    rel_path=rel_path,
                    filename=path.name,
                    artist=artist,
                    album=album,
                    title=title,
                    track_number=track_number,
                )
            )
            idx += 1

        with self._lock:
            self._tracks = found
            self._by_id = {t.id: t for t in found}

        return len(found)

    def all_tracks(self) -> list[Track]:
        with self._lock:
            return list(self._tracks)

    def get(self, track_id: int) -> Optional[Track]:
        with self._lock:
            return self._by_id.get(track_id)

    def search(self, query: str, limit: int = 500) -> list[Track]:
        q = query.strip().lower()
        if not q:
            return self.all_tracks()[:limit]

        terms = [term for term in re.split(r"\s+", q) if term]
        results: list[Track] = []
        for track in self.all_tracks():
            haystack = " | ".join([
                track.artist,
                track.album,
                track.title,
                track.filename,
                track.rel_path,
            ]).lower()
            if all(term in haystack for term in terms):
                results.append(track)
                if len(results) >= limit:
                    break
        return results


class WalkmanTarget:
    def __init__(self, walkman_root: Path, music_root: Path, playlist_dir: Path) -> None:
        self.walkman_root = walkman_root.resolve()
        self.music_root = music_root.resolve()
        self.playlist_dir = playlist_dir.resolve()


def detect_walkman_root() -> Optional[Path]:
    volumes = Path("/Volumes")
    if not volumes.exists():
        return None

    preferred: list[tuple[int, Path]] = []
    for child in volumes.iterdir():
        if not child.is_dir():
            continue
        name = child.name.lower()
        score = 0
        if "walkman" in name:
            score += 100
        if "sony" in name:
            score += 50
        if score:
            preferred.append((score, child))

    if preferred:
        preferred.sort(key=lambda item: (-item[0], item[1].name.lower()))
        return preferred[0][1]

    return None


def pick_existing_subdir(root: Path, candidates: list[str]) -> Optional[Path]:
    for rel in candidates:
        candidate = root / rel
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def resolve_walkman_target(walkman_root: Optional[Path], playlist_subdir: Optional[str]) -> WalkmanTarget:
    root = walkman_root.resolve() if walkman_root else detect_walkman_root()
    if root is None:
        raise SystemExit(
            "Could not auto-detect the Walkman mount under /Volumes. "
            "Run again with --walkman-root '/Volumes/YourWalkmanName'."
        )

    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Walkman root does not exist or is not a directory: {root}")

    music_root = pick_existing_subdir(root, DEFAULT_MUSIC_DIR_CANDIDATES)
    if music_root is None:
        raise SystemExit(
            f"Could not find a music folder on the Walkman under {root}. "
            "Expected something like MUSIC/"
        )

    if playlist_subdir:
        playlist_dir = root / playlist_subdir
    else:
        playlist_dir = pick_existing_subdir(root, DEFAULT_PLAYLIST_DIR_CANDIDATES) or (root / "Playlists")

    playlist_dir.mkdir(parents=True, exist_ok=True)
    return WalkmanTarget(root, music_root, playlist_dir)


HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Walkman Playlist Builder</title>
  <style>
    body { font-family: Arial, Helvetica, sans-serif; margin: 0; background: #111; color: #eee; }
    .wrap { max-width: 1400px; margin: 0 auto; padding: 16px; }
    h1 { margin: 0 0 12px; font-size: 24px; }
    .bar { display: grid; grid-template-columns: 1.2fr 220px 160px 160px auto; gap: 8px; margin-bottom: 12px; }
    input, button { padding: 10px; border-radius: 8px; border: 1px solid #444; background: #1b1b1b; color: #eee; }
    button { cursor: pointer; }
    button:hover { background: #252525; }
    .layout { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 16px; }
    .panel { background: #171717; border: 1px solid #2f2f2f; border-radius: 12px; padding: 12px; }
    .list { max-height: 72vh; overflow: auto; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #2b2b2b; font-size: 14px; vertical-align: top; }
    th { position: sticky; top: 0; background: #171717; }
    .muted { color: #aaa; }
    .small { font-size: 12px; }
    .rowbtn { white-space: nowrap; }
    .status { margin-top: 10px; min-height: 20px; color: #9bd; }
    .pill { display: inline-block; padding: 3px 8px; border-radius: 999px; background: #2b2b2b; font-size: 12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Walkman Playlist Builder</h1>
    <div class="muted small" id="deviceInfo" style="margin-bottom:12px;"></div>
    <div class="bar">
      <input id="search" placeholder="Search artist, album, title, filename, or path">
      <input id="playlistName" placeholder="Playlist name" value="New Playlist">
      <button onclick="reloadTracks()">Refresh Search</button>
      <button onclick="rescanLibrary()">Rescan Library</button>
      <button onclick="savePlaylist()">Save Playlist</button>
    </div>

    <div class="layout">
      <div class="panel">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
          <strong>Library</strong>
          <span class="pill" id="trackCount">0 tracks</span>
        </div>
        <div class="list">
          <table>
            <thead>
              <tr>
                <th>Artist</th>
                <th>Album</th>
                <th>Title</th>
                <th>Track</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="results"></tbody>
          </table>
        </div>
      </div>

      <div class="panel">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
          <strong>Playlist</strong>
          <span class="pill" id="playlistCount">0 selected</span>
        </div>
        <div class="list">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Track</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="playlist"></tbody>
          </table>
        </div>
        <div class="status" id="status"></div>
      </div>
    </div>
  </div>

<script>
let playlist = [];

function esc(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', '&quot;');
}

function setStatus(msg, isError=false) {
  const el = document.getElementById('status');
  el.textContent = msg;
  el.style.color = isError ? '#f88' : '#9bd';
}

function renderPlaylist() {
  const body = document.getElementById('playlist');
  body.innerHTML = playlist.map((t, idx) => `
    <tr>
      <td>${idx + 1}</td>
      <td>
        <div><strong>${esc(t.artist)} — ${esc(t.title)}</strong></div>
        <div class="muted small">${esc(t.album)}</div>
        <div class="muted small">${esc(t.rel_path)}</div>
      </td>
      <td class="rowbtn">
        <button onclick="moveUp(${idx})">↑</button>
        <button onclick="moveDown(${idx})">↓</button>
        <button onclick="removeAt(${idx})">Remove</button>
      </td>
    </tr>
  `).join('');
  document.getElementById('playlistCount').textContent = `${playlist.length} selected`;
}

function addTrack(track) {
  playlist.push(track);
  renderPlaylist();
}

function removeAt(idx) {
  playlist.splice(idx, 1);
  renderPlaylist();
}

function moveUp(idx) {
  if (idx <= 0) return;
  [playlist[idx - 1], playlist[idx]] = [playlist[idx], playlist[idx - 1]];
  renderPlaylist();
}

function moveDown(idx) {
  if (idx >= playlist.length - 1) return;
  [playlist[idx + 1], playlist[idx]] = [playlist[idx], playlist[idx + 1]];
  renderPlaylist();
}

async async function loadDeviceInfo() {
  const resp = await fetch('/api/device_info');
  const data = await resp.json();
  document.getElementById('deviceInfo').textContent = `Walkman: ${data.walkman_root} | Music: ${data.music_root} | Playlists: ${data.playlist_dir}`;
}

async function reloadTracks() {
  const q = document.getElementById('search').value.trim();
  const resp = await fetch(`/api/search?q=${encodeURIComponent(q)}`);
  const data = await resp.json();
  document.getElementById('trackCount').textContent = `${data.total} tracks`;
  const body = document.getElementById('results');
  body.innerHTML = data.results.map(t => `
    <tr>
      <td>${esc(t.artist)}</td>
      <td>${esc(t.album)}</td>
      <td>
        <div><strong>${esc(t.title)}</strong></div>
        <div class="muted small">${esc(t.rel_path)}</div>
      </td>
      <td>${esc(t.track_number)}</td>
      <td class="rowbtn"><button onclick="fetchTrack(${t.id})">Add</button></td>
    </tr>
  `).join('');
}

async function fetchTrack(id) {
  const resp = await fetch(`/api/track/${id}`);
  const track = await resp.json();
  playlist.push(track);
  renderPlaylist();
}

async function rescanLibrary() {
  setStatus('Rescanning library...');
  const resp = await fetch('/api/rescan', { method: 'POST' });
  const data = await resp.json();
  setStatus(`Scan complete: ${data.count} tracks found.`);
  await reloadTracks();
}

async function savePlaylist() {
  const name = document.getElementById('playlistName').value.trim();
  if (!name) {
    setStatus('Enter a playlist name first.', true);
    return;
  }
  if (!playlist.length) {
    setStatus('Add at least one track first.', true);
    return;
  }

  const resp = await fetch('/api/save_playlist', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name,
      track_ids: playlist.map(t => t.id)
    })
  });
  const data = await resp.json();
  if (!resp.ok) {
    setStatus(data.error || 'Failed to save playlist.', true);
    return;
  }
  setStatus(`Saved: ${data.path}`);
}

const searchEl = document.getElementById('search');
let timer = null;
searchEl.addEventListener('input', () => {
  clearTimeout(timer);
  timer = setTimeout(reloadTracks, 180);
});

loadDeviceInfo();
reloadTracks();
renderPlaylist();
</script>
</body>
</html>
"""


def sanitize_playlist_name(name: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or "New Playlist"


def create_app(library: TrackLibrary, target: WalkmanTarget) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index() -> Response:
        return Response(HTML, mimetype="text/html")

    @app.get("/api/device_info")
    def api_device_info():
        return jsonify({
            "walkman_root": str(target.walkman_root),
            "music_root": str(target.music_root),
            "playlist_dir": str(target.playlist_dir),
        })

    @app.get("/api/search")
    def api_search():
        query = request.args.get("q", "")
        results = [asdict(t) for t in library.search(query)]
        return jsonify({
            "total": len(library.all_tracks()),
            "results": results,
        })

    @app.get("/api/track/<int:track_id>")
    def api_track(track_id: int):
        track = library.get(track_id)
        if not track:
            return jsonify({"error": "Track not found"}), 404
        return jsonify(asdict(track))

    @app.post("/api/rescan")
    def api_rescan():
        count = library.scan()
        return jsonify({"count": count})

    @app.post("/api/save_playlist")
    def api_save_playlist():
        payload = request.get_json(silent=True) or {}
        name = sanitize_playlist_name(str(payload.get("name", "New Playlist")))
        track_ids = payload.get("track_ids", [])

        if not isinstance(track_ids, list) or not track_ids:
            return jsonify({"error": "No tracks selected"}), 400

        tracks: list[Track] = []
        for raw_id in track_ids:
            try:
                track_id = int(raw_id)
            except (TypeError, ValueError):
                return jsonify({"error": f"Invalid track id: {raw_id}"}), 400
            track = library.get(track_id)
            if not track:
                return jsonify({"error": f"Track not found: {track_id}"}), 404
            tracks.append(track)

        target.playlist_dir.mkdir(parents=True, exist_ok=True)
        out_path = target.playlist_dir / f"{name}.m3u8"

        lines = ["#EXTM3U"]
        for track in tracks:
            display_name = f"{track.artist} - {track.title}"
            lines.append(f"#EXTINF:-1,{display_name}")

            abs_track_path = Path(track.path)
            rel_path = os.path.relpath(abs_track_path, start=out_path.parent)
            lines.append(rel_path)

        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        return jsonify({
            "ok": True,
            "path": str(out_path),
            "count": len(tracks),
        })

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Web-based Walkman playlist builder")
    parser.add_argument("--walkman-root", default=None, help="Mounted Walkman root, e.g. /Volumes/WALKMAN")
    parser.add_argument(
        "--playlist-subdir",
        default=None,
        help="Playlist folder relative to the Walkman root. Defaults to an auto-detected playlist folder or /Playlists.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the web server")
    parser.add_argument("--port", type=int, default=5050, help="Port to bind the web server")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    target = resolve_walkman_target(
        Path(args.walkman_root).expanduser() if args.walkman_root else None,
        args.playlist_subdir,
    )

    library = TrackLibrary(target.music_root)
    count = library.scan()
    print(f"Walkman root: {target.walkman_root}")
    print(f"Scanning tracks from: {target.music_root}")
    print(f"Playlist output directory: {target.playlist_dir}")
    print(f"Scanned {count} tracks")

    app = create_app(library, target)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
