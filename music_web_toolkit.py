#!/usr/bin/env python3
"""Walkman Playlist Studio - simple web UI for creating/editing playlists."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string, request

from walkman_playlist_writer import resolve_music_root, write_walkman_playlist

app = Flask(__name__)

CACHE: dict[str, Any] = {
    "music_root": None,
    "tracks": [],  # list[dict(abs, rel)]
}


def _scan_tracks(music_root: Path) -> list[dict[str, str]]:
    tracks: list[dict[str, str]] = []
    audio_ext = {".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".alac"}
    for p in sorted(music_root.rglob("*")):
        if p.is_file() and p.suffix.lower() in audio_ext:
            tracks.append({
                "abs": str(p.resolve()),
                "rel": str(p.relative_to(music_root)).replace("\\", "/"),
            })
    return tracks


def _list_playlists(music_root: Path) -> list[str]:
    return sorted(
        p.stem
        for p in music_root.glob("*.m3u")
        if not p.name.startswith("._") and not p.name.startswith(".")
    )


def _read_playlist_selected(music_root: Path, playlist_name: str) -> list[str]:
    path = music_root / f"{playlist_name}.m3u"
    if not path.exists():
        return []
    selected: list[str] = []
    for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        target = (music_root / item).resolve()
        if target.exists() and target.is_file():
            selected.append(str(target))
    return selected


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Walkman Playlist Studio</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin:0; background:#f4f6fb; color:#1f2937; }
    .wrap { max-width:1200px; margin:0 auto; padding:20px; }
    .card { background:#fff; border-radius:12px; padding:14px; margin:12px 0; box-shadow:0 2px 8px rgba(0,0,0,.08); }
    .row { display:flex; gap:10px; flex-wrap:wrap; }
    .col { flex:1; min-width:260px; }
    input, select, button { font-size:16px; padding:8px; border:1px solid #cbd5e1; border-radius:8px; width:100%; box-sizing:border-box; }
    button { background:#2563eb; color:#fff; border:none; cursor:pointer; width:auto; }
    button.secondary { background:#6b7280; }
    button.ghost { background:#e5e7eb; color:#111827; }
    .songs { height:460px; overflow:auto; border:1px solid #e5e7eb; border-radius:8px; padding:8px; }
    .song { padding:5px; border-bottom:1px solid #f1f5f9; }
    .badge { display:inline-block; background:#eef2ff; color:#3730a3; border-radius:999px; padding:4px 10px; margin-right:8px; }
    .muted { color:#6b7280; }
    .status { white-space:pre-wrap; }
  </style>
</head>
<body>
<div class="wrap">
  <h1>🎵 Walkman Playlist Studio</h1>
  <div class="muted">Scan songs, create new playlists, or load/edit existing playlists.</div>

  <div class="card">
    <div class="row">
      <div class="col">
        <label>Walkman root</label>
        <input id="walkmanRoot" value="/Volumes/MyMusic" />
      </div>
      <div class="col">
        <label>Music subdir (optional)</label>
        <input id="musicSubdir" placeholder="MUSIC" />
      </div>
      <div style="align-self:end;">
        <button onclick="refreshLibrary()">Refresh library</button>
      </div>
    </div>
    <div style="margin-top:8px;">
      <span class="badge" id="songCount">0 songs</span>
      <span class="badge" id="selCount">0 selected</span>
      <span class="badge" id="playlistCount">0 playlists</span>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <div class="col">
        <label>Create new playlist</label>
        <input id="newName" placeholder="MyHead" />
      </div>
      <div class="col">
        <label>Edit existing playlist</label>
        <select id="existing"></select>
      </div>
    </div>
    <div class="row" style="margin-top:10px;">
      <button onclick="loadExisting()" class="secondary">Load selected playlist</button>
      <button onclick="saveNew()">Save as new playlist</button>
      <button onclick="saveExisting()">Save changes to loaded playlist</button>
      <button onclick="selectVisible()" class="ghost">Select all visible</button>
      <button onclick="clearSelection()" class="ghost">Clear selection</button>
    </div>
    <div class="muted" style="margin-top:8px;">Playlists are stored in the Walkman MUSIC folder only.</div>
  </div>

  <div class="card">
    <label>Search songs (works with large libraries)</label>
    <input id="search" placeholder="Type artist, album, or track..." oninput="searchTracks()" />
    <div id="songs" class="songs" style="margin-top:8px;"></div>
  </div>

  <div class="card status" id="status">Ready.</div>
</div>

<script>
let selected = new Set();
let visible = [];   // current visible rows [{abs, rel}]
let loadedPlaylistName = null;

function cfg(){
  return {
    walkman_root: document.getElementById('walkmanRoot').value.trim(),
    music_subdir: document.getElementById('musicSubdir').value.trim(),
  }
}
function setStatus(t){ document.getElementById('status').textContent = t; }
function updateCounts(meta){
  document.getElementById('songCount').textContent = `${meta.track_count} songs`;
  document.getElementById('playlistCount').textContent = `${meta.playlist_count} playlists`;
  document.getElementById('selCount').textContent = `${selected.size} selected`;
}

async function refreshLibrary(){
  setStatus('Scanning library...');
  const res = await fetch('/api/library', {
    method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(cfg())
  });
  const data = await res.json();
  if(!res.ok){ setStatus('Error: '+(data.error||'scan failed')); return; }

  const sel = document.getElementById('existing');
  sel.innerHTML = '';
  for (const name of data.playlists){
    const o = document.createElement('option'); o.value = name; o.textContent = name; sel.appendChild(o);
  }
  selected.clear();
  loadedPlaylistName = null;
  updateCounts(data);
  await searchTracks();
  setStatus(`Loaded ${data.track_count} songs from ${data.music_root}`);
}

async function searchTracks(){
  const q = document.getElementById('search').value.trim();
  const p = new URLSearchParams({...cfg(), q, limit: '1000'});
  const res = await fetch(`/api/tracks?${p}`);
  const data = await res.json();
  if(!res.ok){ setStatus('Error: '+(data.error||'search failed')); return; }
  visible = data.tracks;
  const host = document.getElementById('songs');
  host.innerHTML = '';
  for(const t of visible){
    const d = document.createElement('div');
    d.className = 'song';
    const checked = selected.has(t.abs) ? 'checked' : '';
    d.innerHTML = `<input type="checkbox" ${checked} data-abs="${t.abs}"> ${t.rel}`;
    const cb = d.querySelector('input');
    cb.addEventListener('change', () => {
      if(cb.checked) selected.add(t.abs); else selected.delete(t.abs);
      document.getElementById('selCount').textContent = `${selected.size} selected`;
    });
    host.appendChild(d);
  }
  document.getElementById('selCount').textContent = `${selected.size} selected`;
}

function selectVisible(){
  visible.forEach(t => selected.add(t.abs));
  searchTracks();
}
function clearSelection(){
  selected.clear();
  searchTracks();
}

async function loadExisting(){
  const name = document.getElementById('existing').value;
  if(!name){ setStatus('Select a playlist first.'); return; }
  const p = new URLSearchParams({...cfg(), playlist_name:name});
  const res = await fetch(`/api/playlist?${p}`);
  const data = await res.json();
  if(!res.ok){ setStatus('Error: '+(data.error||'load failed')); return; }
  selected = new Set(data.selected_tracks);
  loadedPlaylistName = name;
  await searchTracks();
  setStatus(`Loaded playlist '${name}' (${selected.size} tracks)`);
}

async function saveNew(){
  const name = document.getElementById('newName').value.trim();
  if(!name){ setStatus('Enter a new playlist name.'); return; }
  const existing = Array.from(document.getElementById('existing').options).map(o => o.value.toLowerCase());
  if(existing.includes(name.toLowerCase())){
    setStatus(`Cannot create '${name}': playlist name already exists. Choose a different name or edit existing.`);
    return;
  }
  await savePlaylist(name, false);
}

async function saveExisting(){
  if(!loadedPlaylistName){
    setStatus('Load an existing playlist first, then save changes.');
    return;
  }
  await savePlaylist(loadedPlaylistName, true);
}

async function savePlaylist(name, editing){
  const payload = {...cfg(), playlist_name: name, tracks: Array.from(selected)};
  const res = await fetch('/api/save', {
    method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)
  });
  const data = await res.json();
  if(!res.ok){ setStatus('Error: '+(data.error||'save failed')); return; }
  await refreshLibrary();
  if(editing){
    document.getElementById('existing').value = name;
    loadedPlaylistName = name;
  }
  setStatus(`Saved '${name}' in MUSIC. kept=${data.tracks_kept}, skipped=${data.tracks_skipped}`);
}

refreshLibrary();
</script>
</body>
</html>
"""


@app.route("/")
def home() -> str:
    return render_template_string(HTML)


@app.route("/api/library", methods=["POST"])
def api_library():
    try:
        data = request.get_json(force=True)
        walkman_root = Path(str(data.get("walkman_root", "/Volumes/MyMusic"))).expanduser().resolve()
        music_subdir = str(data.get("music_subdir", "")).strip() or None
        music_root = resolve_music_root(walkman_root, music_subdir)

        tracks = _scan_tracks(music_root)
        CACHE["music_root"] = str(music_root)
        CACHE["tracks"] = tracks

        playlists = _list_playlists(music_root)
        return jsonify(
            {
                "music_root": str(music_root),
                "track_count": len(tracks),
                "playlist_count": len(playlists),
                "playlists": playlists,
            }
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/tracks")
def api_tracks():
    try:
        q = request.args.get("q", "").strip().lower()
        limit = int(request.args.get("limit", "1000"))
        tracks = CACHE.get("tracks") or []
        if q:
            tracks = [t for t in tracks if q in t["rel"].lower()]
        return jsonify({"tracks": tracks[: max(1, min(limit, 5000))]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/playlist")
def api_playlist():
    try:
        walkman_root = Path(request.args.get("walkman_root", "/Volumes/MyMusic")).expanduser().resolve()
        music_subdir = request.args.get("music_subdir", "").strip() or None
        name = request.args.get("playlist_name", "").strip()
        if not name:
            return jsonify({"error": "playlist_name is required"}), 400

        music_root = resolve_music_root(walkman_root, music_subdir)
        selected = _read_playlist_selected(music_root, name)
        return jsonify({"selected_tracks": selected})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/save", methods=["POST"])
def api_save():
    try:
        data = request.get_json(force=True)
        walkman_root = Path(str(data.get("walkman_root", "/Volumes/MyMusic"))).expanduser().resolve()
        music_subdir = str(data.get("music_subdir", "")).strip() or None
        playlist_name = str(data.get("playlist_name", "")).strip()
        if not playlist_name:
            return jsonify({"error": "playlist_name is required"}), 400

        selected = [Path(p) for p in data.get("tracks", []) if str(p).strip()]
        out_file, kept, skipped = write_walkman_playlist(
            walkman_root=walkman_root,
            playlist_name=playlist_name,
            selected_tracks=selected,
            music_subdir=music_subdir,
            output_dir=None,  # MUSIC folder by default
            overwrite=True,
        )
        return jsonify({"playlist": str(out_file), "tracks_kept": kept, "tracks_skipped": skipped})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Walkman Playlist Studio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5055)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)
