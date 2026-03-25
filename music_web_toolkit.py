#!/usr/bin/env python3
"""Single web-based toolkit for Walkman music workflows.

Features:
- Build/validate .m3u playlists
- Run fast missing-track comparison reports
- Plan/apply metadata-based renames (dry-run default)
- Inspect local SQLite DB summary (default: ./music_inventory.db)
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from flask import Flask, redirect, render_template_string, request, url_for

from walkman_toolkit.pathing import resolve_walkman_music_root
from walkman_toolkit.playlist import collect_tracks, validate_m3u, write_m3u
from walkman_toolkit.rename_ops import apply_plans, plan_renames
from walkman_toolkit.reporting import write_csv, write_json, write_text
from walkman_toolkit.scanning import scan_library_fast

app = Flask(__name__)

HOME_TEMPLATE = """
<!doctype html>
<title>Music + Walkman Toolkit</title>
<h1>Music + Walkman Toolkit</h1>
<p>Conservative report-first workflow. Playlist creation is prioritized.</p>
<ul>
  <li><a href="{{ url_for('playlist_page') }}">Build Walkman Playlist (.m3u)</a></li>
  <li><a href="{{ url_for('missing_page') }}">Missing Track Report</a></li>
  <li><a href="{{ url_for('rename_page') }}">Rename from Metadata</a></li>
  <li><a href="{{ url_for('db_page') }}">Database Summary</a></li>
</ul>
"""

PLAYLIST_TEMPLATE = """
<!doctype html>
<title>Build Playlist</title>
<h1>Build Walkman Playlist (.m3u)</h1>
<form method="post">
  <label>Walkman root <input name="walkman_root" value="{{ walkman_root }}" size="80"></label><br>
  <label>Music subdir (optional) <input name="music_subdir" value="{{ music_subdir }}" size="40"></label><br>
  <label>Playlist folder (default: WALKMAN/PLAYLISTS) <input name="playlist_dir" value="{{ playlist_dir }}" size="80"></label><br>
  <label>Playlist name <input name="playlist_name" value="{{ playlist_name }}" size="40"></label><br>
  <label>Source roots (one per line, optional)</label><br>
  <textarea name="source_roots" rows="5" cols="100">{{ source_roots }}</textarea><br>
  <label><input type="checkbox" name="use_db" {% if use_db %}checked{% endif %}> Load tracks from SQLite DB</label><br>
  <label>DB path <input name="db_path" value="{{ db_path }}" size="80"></label><br>
  <label>DB table <input name="db_table" value="{{ db_table }}" size="40"></label><br>
  <label>DB path column <input name="db_path_column" value="{{ db_path_column }}" size="40"></label><br>
  <label>DB base root (optional, for relative DB paths) <input name="db_base_root" value="{{ db_base_root }}" size="80"></label><br>
  <label>DB WHERE clause (optional, no 'WHERE' keyword) <input name="db_where" value="{{ db_where }}" size="100"></label><br>
  <label><input type="checkbox" name="validate" {% if validate %}checked{% endif %}> Validate after write</label><br>
  <button type="submit">Build Playlist</button>
</form>
<p><a href="{{ url_for('home') }}">Back</a></p>
{% if result %}<pre>{{ result }}</pre>{% endif %}
"""

MISSING_TEMPLATE = """
<!doctype html>
<title>Missing Report</title>
<h1>Missing Track Report</h1>
<form method="post">
  <label>Walkman root <input name="walkman_root" value="{{ walkman_root }}" size="80"></label><br>
  <label>Music subdir (optional) <input name="music_subdir" value="{{ music_subdir }}" size="40"></label><br>
  <label>Source roots (one per line)</label><br>
  <textarea name="source_roots" rows="5" cols="100">{{ source_roots }}</textarea><br>
  <label>Report prefix <input name="report_prefix" value="{{ report_prefix }}" size="40"></label><br>
  <button type="submit">Run Report</button>
</form>
<p><a href="{{ url_for('home') }}">Back</a></p>
{% if result %}<pre>{{ result }}</pre>{% endif %}
"""

RENAME_TEMPLATE = """
<!doctype html>
<title>Rename Planner</title>
<h1>Rename from Metadata (Conservative)</h1>
<form method="post">
  <label>Walkman root <input name="walkman_root" value="{{ walkman_root }}" size="80"></label><br>
  <label>Music subdir (optional) <input name="music_subdir" value="{{ music_subdir }}" size="40"></label><br>
  <label>Skip artists (one per line)</label><br>
  <textarea name="skip_artists" rows="5" cols="100">{{ skip_artists }}</textarea><br>
  <label>Report JSON path <input name="report_json" value="{{ report_json }}" size="60"></label><br>
  <label><input type="checkbox" name="apply"> Apply renames (unchecked = dry-run)</label><br>
  <button type="submit">Run Rename Planner</button>
</form>
<p><a href="{{ url_for('home') }}">Back</a></p>
{% if result %}<pre>{{ result }}</pre>{% endif %}
"""

DB_TEMPLATE = """
<!doctype html>
<title>DB Summary</title>
<h1>Database Summary</h1>
<form method="post">
  <label>DB path <input name="db_path" value="{{ db_path }}" size="80"></label>
  <button type="submit">Inspect</button>
</form>
<p><a href="{{ url_for('home') }}">Back</a></p>
{% if result %}<pre>{{ result }}</pre>{% endif %}
{% if suggestions %}
<h2>Likely track path sources</h2>
<pre>{{ suggestions }}</pre>
{% endif %}
"""


def _split_lines(value: str) -> list[str]:
    return [line.strip() for line in value.splitlines() if line.strip()]


def _load_tracks_from_db(
    db_path: Path,
    table: str,
    path_column: str,
    where_clause: str,
    base_root: Path | None,
) -> list[Path]:
    if not db_path.exists():
        raise FileNotFoundError(f"DB does not exist: {db_path}")
    if not table or not path_column:
        raise ValueError("DB table and DB path column are required when using DB source mode.")

    query = f'SELECT "{path_column}" FROM "{table}"'
    if where_clause.strip():
        query = f"{query} WHERE {where_clause.strip()}"

    tracks: list[Path] = []
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for row in cur.execute(query):
            raw = str(row[0] or "").strip()
            if not raw:
                continue
            candidate = Path(raw).expanduser()
            if not candidate.is_absolute():
                if base_root is not None:
                    candidate = base_root / candidate
            candidate = candidate.resolve()
            if candidate.exists() and candidate.is_file():
                tracks.append(candidate)
    return tracks


def _suggest_db_track_sources(db_path: Path) -> list[str]:
    """Suggest likely (table, column) sources that contain audio file paths."""
    audio_markers = (".mp3", ".flac", ".m4a", ".aac", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".alac")
    suggestions: list[str] = []
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        tables = [row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        for table in tables:
            columns = [row[1] for row in cur.execute(f"PRAGMA table_info('{table}')")]
            for column in columns:
                lowered = column.lower()
                likely_name = any(token in lowered for token in ("path", "file", "filename", "location", "uri"))
                if not likely_name:
                    continue
                query = f'SELECT "{column}" FROM "{table}" WHERE "{column}" IS NOT NULL LIMIT 200'
                sample = [str(row[0] or "").lower() for row in cur.execute(query)]
                non_empty = [value for value in sample if value.strip()]
                if not non_empty:
                    continue
                audio_like = sum(1 for value in non_empty if value.endswith(audio_markers))
                if audio_like == 0:
                    continue
                ratio = audio_like / len(non_empty)
                suggestions.append(
                    f"{table}.{column}  (audio-like sample ratio: {audio_like}/{len(non_empty)} = {ratio:.0%})"
                )
    return suggestions


@app.route("/")
def home() -> str:
    return render_template_string(HOME_TEMPLATE)


@app.route("/playlist", methods=["GET", "POST"])
def playlist_page() -> str:
    ctx = {
        "walkman_root": "/Volumes/MyMusic",
        "music_subdir": "",
        "playlist_dir": "",
        "playlist_name": "All Tracks",
        "source_roots": "",
        "use_db": False,
        "db_path": "music_inventory.db",
        "db_table": "",
        "db_path_column": "",
        "db_base_root": "",
        "db_where": "",
        "validate": True,
        "result": "",
    }
    if request.method == "POST":
        try:
            walkman_root = Path(request.form["walkman_root"]).expanduser().resolve()
            music_subdir = request.form.get("music_subdir") or None
            playlist_name = request.form["playlist_name"].strip() or "All Tracks"
            playlist_dir_raw = request.form.get("playlist_dir", "").strip()
            playlist_dir = Path(playlist_dir_raw).expanduser().resolve() if playlist_dir_raw else (walkman_root / "PLAYLISTS")
            playlist_path = playlist_dir / f"{playlist_name}.m3u"
            validate = bool(request.form.get("validate"))
            use_db = bool(request.form.get("use_db"))

            music_root = resolve_walkman_music_root(walkman_root, music_subdir)
            if use_db:
                db_path = Path(request.form.get("db_path", "music_inventory.db")).expanduser().resolve()
                db_table = request.form.get("db_table", "").strip()
                db_path_column = request.form.get("db_path_column", "").strip()
                db_base_root_raw = request.form.get("db_base_root", "").strip()
                db_where = request.form.get("db_where", "").strip()
                db_base_root = Path(db_base_root_raw).expanduser().resolve() if db_base_root_raw else None
                tracks = _load_tracks_from_db(
                    db_path=db_path,
                    table=db_table,
                    path_column=db_path_column,
                    where_clause=db_where,
                    base_root=db_base_root,
                )
            else:
                source_roots = _split_lines(request.form.get("source_roots", ""))
                roots = [Path(raw).expanduser().resolve() for raw in source_roots] if source_roots else [music_root]
                tracks = []
                for root in roots:
                    if root.exists() and root.is_dir():
                        tracks.extend(collect_tracks(root))

            track_map = {str(track.resolve()): track for track in tracks}
            ordered_tracks = [track_map[key] for key in sorted(track_map)]
            write_m3u(playlist_path, ordered_tracks, relative=True)

            details = [
                f"Wrote: {playlist_path}",
                f"Tracks: {len(ordered_tracks)}",
                f"Source mode: {'database' if use_db else 'filesystem'}",
            ]
            if validate:
                validation = validate_m3u(playlist_path)
                details.extend(
                    [
                        f"Playlist exists: {validation.playlist_exists}",
                        f"Missing references: {len(validation.missing_files)}",
                        f"Backslash paths: {len(validation.bad_paths)}",
                    ]
                )
            ctx["result"] = "\n".join(details)
        except Exception as exc:  # surface error in UI
            logging.exception("playlist build failed")
            ctx["result"] = f"Error: {exc}"

        ctx.update(
            {
                "walkman_root": request.form.get("walkman_root", ctx["walkman_root"]),
                "music_subdir": request.form.get("music_subdir", ""),
                "playlist_dir": request.form.get("playlist_dir", ""),
                "playlist_name": request.form.get("playlist_name", ctx["playlist_name"]),
                "source_roots": request.form.get("source_roots", ""),
                "use_db": bool(request.form.get("use_db")),
                "db_path": request.form.get("db_path", "music_inventory.db"),
                "db_table": request.form.get("db_table", ""),
                "db_path_column": request.form.get("db_path_column", ""),
                "db_base_root": request.form.get("db_base_root", ""),
                "db_where": request.form.get("db_where", ""),
                "validate": bool(request.form.get("validate")),
            }
        )
    return render_template_string(PLAYLIST_TEMPLATE, **ctx)


@app.route("/missing", methods=["GET", "POST"])
def missing_page() -> str:
    ctx = {
        "walkman_root": "/Volumes/MyMusic",
        "music_subdir": "",
        "source_roots": "",
        "report_prefix": "walkman_missing_fast",
        "result": "",
    }
    if request.method == "POST":
        try:
            walkman_root = Path(request.form["walkman_root"]).expanduser().resolve()
            music_subdir = request.form.get("music_subdir") or None
            sources = [Path(v).expanduser().resolve() for v in _split_lines(request.form.get("source_roots", ""))]
            report_prefix = request.form.get("report_prefix", "walkman_missing_fast").strip() or "walkman_missing_fast"

            music_root = resolve_walkman_music_root(walkman_root, music_subdir)
            _, walkman_keys = scan_library_fast(music_root)

            missing: list[dict[str, str]] = []
            for source in sources:
                if not source.exists() or not source.is_dir():
                    continue
                rows, _ = scan_library_fast(source)
                for row in rows:
                    if row.key not in walkman_keys:
                        missing.append(
                            {
                                "root": row.root,
                                "rel_path": row.rel_path,
                                "ext": row.ext,
                                "path": row.path,
                                "key": row.key,
                            }
                        )

            txt = Path(f"{report_prefix}.txt").resolve()
            csv_path = Path(f"{report_prefix}.csv").resolve()
            json_path = Path(f"{report_prefix}.json").resolve()

            lines = [
                f"Walkman music root: {music_root}",
                f"Sources: {', '.join(str(s) for s in sources)}",
                f"Missing tracks: {len(missing)}",
            ]
            write_text(txt, "\n".join(lines) + "\n")
            write_csv(csv_path, missing, ["root", "rel_path", "ext", "path", "key"])
            write_json(json_path, {"missing_count": len(missing), "missing_tracks": missing})
            ctx["result"] = f"Missing tracks: {len(missing)}\nText: {txt}\nCSV: {csv_path}\nJSON: {json_path}"
        except Exception as exc:
            logging.exception("missing report failed")
            ctx["result"] = f"Error: {exc}"

        ctx.update(
            {
                "walkman_root": request.form.get("walkman_root", ctx["walkman_root"]),
                "music_subdir": request.form.get("music_subdir", ""),
                "source_roots": request.form.get("source_roots", ""),
                "report_prefix": request.form.get("report_prefix", ctx["report_prefix"]),
            }
        )
    return render_template_string(MISSING_TEMPLATE, **ctx)


@app.route("/rename", methods=["GET", "POST"])
def rename_page() -> str:
    ctx = {
        "walkman_root": "/Volumes/MyMusic",
        "music_subdir": "",
        "skip_artists": "Richard Lorson",
        "report_json": "walkman_rename_report.json",
        "result": "",
    }
    if request.method == "POST":
        try:
            walkman_root = Path(request.form["walkman_root"]).expanduser().resolve()
            music_subdir = request.form.get("music_subdir") or None
            skip_artists = {v.lower() for v in _split_lines(request.form.get("skip_artists", ""))}
            report_json = Path(request.form.get("report_json", "walkman_rename_report.json")).expanduser().resolve()
            apply = bool(request.form.get("apply"))

            music_root = resolve_walkman_music_root(walkman_root, music_subdir)
            plans = plan_renames(music_root, skip_artists)
            moved, skipped = apply_plans(plans, dry_run=not apply)

            payload = {
                "dry_run": not apply,
                "planned": len(plans),
                "moved": moved,
                "skipped": skipped,
                "plans": [
                    {"source": str(plan.source), "destination": str(plan.destination), "reason": plan.reason}
                    for plan in plans
                ],
            }
            write_json(report_json, payload)
            ctx["result"] = f"Planned: {len(plans)}\nMoved: {moved}\nSkipped: {skipped}\nReport: {report_json}"
        except Exception as exc:
            logging.exception("rename failed")
            ctx["result"] = f"Error: {exc}"

        ctx.update(
            {
                "walkman_root": request.form.get("walkman_root", ctx["walkman_root"]),
                "music_subdir": request.form.get("music_subdir", ""),
                "skip_artists": request.form.get("skip_artists", ""),
                "report_json": request.form.get("report_json", ctx["report_json"]),
            }
        )
    return render_template_string(RENAME_TEMPLATE, **ctx)


@app.route("/db", methods=["GET", "POST"])
def db_page() -> str:
    ctx = {
        "db_path": "music_inventory.db",
        "result": "",
        "suggestions": "",
    }
    if request.method == "POST":
        db_path = Path(request.form.get("db_path", "music_inventory.db")).expanduser().resolve()
        try:
            if not db_path.exists():
                raise FileNotFoundError(f"DB does not exist: {db_path}")
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                tables = [row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
                lines = [f"DB: {db_path}", f"Tables: {len(tables)}"]
                for table in tables:
                    count = cur.execute(f"SELECT COUNT(*) FROM '{table}'").fetchone()[0]
                    lines.append(f"- {table}: {count}")
            ctx["result"] = "\n".join(lines)
            suggestions = _suggest_db_track_sources(db_path)
            ctx["suggestions"] = "\n".join(suggestions) if suggestions else "No likely track path columns found."
        except Exception as exc:
            logging.exception("db inspect failed")
            ctx["result"] = f"Error: {exc}"
            ctx["suggestions"] = ""
        ctx["db_path"] = request.form.get("db_path", "music_inventory.db")
    return render_template_string(DB_TEMPLATE, **ctx)


@app.route("/home")
def home_redirect() -> str:
    return redirect(url_for("home"))


def parse_args() -> tuple[str, int, bool]:
    import argparse

    parser = argparse.ArgumentParser(description="Run the unified Music + Walkman web toolkit")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5055)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    return args.host, args.port, args.debug


if __name__ == "__main__":
    host, port, debug = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    app.run(host=host, port=port, debug=debug)
