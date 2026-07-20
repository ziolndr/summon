#!/usr/bin/env python3
"""ARBITER live universal-field sidecar.

This is intentionally separate from the core ARBITER server. It never adds or
changes a route on the ARBITER service. It consumes finalized field shards,
builds a continuously growing local search index, and exposes a small search
API plus an optional local SUMMON site.

Default layout:
  ~/ARBITER_UNIVERSAL_FIELD/
    shards/<source>/shard-000001/{manifest.json,metadata.jsonl.gz,vectors.f32,norms.f32}
    live_index/{vectors.f32,norms.f32,metadata.sqlite3,state.json}

Examples:
  python ARBITER_live_field_server.py status

  python ARBITER_live_field_server.py sync \
    --field-dir ~/ARBITER_UNIVERSAL_FIELD \
    --entertainment-dir ~/Downloads/summon_200k_deploy

  python ARBITER_live_field_server.py serve \
    --field-dir ~/ARBITER_UNIVERSAL_FIELD \
    --entertainment-dir ~/Downloads/summon_200k_deploy \
    --assets-dir ~/Downloads/summon_200k_deploy \
    --html ~/Downloads/SUMMON_live_resonance_universal_field.html \
    --embed-url http://127.0.0.1:8000/v1/embed \
    --port 8787
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import hashlib
import json
import math
import mimetypes
import os
import shutil
import sqlite3
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import unquote, urlparse

try:
    import numpy as np
except ImportError as exc:  # pragma: no cover
    raise SystemExit("numpy is required. Run this inside the same venv as the field forge.") from exc

try:
    import fcntl  # macOS/Linux
except ImportError:  # pragma: no cover
    fcntl = None


STATE_SCHEMA = 1
INDEX_SCHEMA = 1
DEFAULT_DIM = 72
DEFAULT_USE_FREQ = True
DEFAULT_CHUNK_ROWS = 250_000
DEFAULT_OVERVIEW_CHARS = 320


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def human_count(value: int) -> str:
    return f"{int(value):,}"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def clamp(value: Any, limit: int) -> str:
    text = clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def safe_json_load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return data


def extract_vectors(payload: Any) -> list[list[float]]:
    if isinstance(payload, dict):
        rows = payload.get("vectors")
        if rows is None:
            rows = payload.get("embeddings")
    else:
        rows = payload
    if not isinstance(rows, list):
        raise ValueError("Unrecognized /v1/embed response")
    return rows


def post_json(url: str, payload: dict[str, Any], timeout: int) -> Any:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            raw = res.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Embed server returned {exc.code}: {detail[:500]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach embed server at {url}: {exc}") from exc
    return json.loads(raw)


@dataclass(frozen=True)
class SourceUnit:
    key: str
    kind: str
    manifest_path: Path
    root_dir: Path
    source: str
    count: int
    dim: int
    use_freq: bool
    vectors_path: Path
    norms_path: Path
    metadata_paths: tuple[Path, ...]
    metadata_format: str
    manifest_sha256: str
    created_at: str


class FileLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        if fcntl is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.handle is not None:
            if fcntl is not None:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
        return False


class LiveFieldIndex:
    def __init__(
        self,
        *,
        field_dir: Path,
        live_dir: Path,
        entertainment_dir: Path | None,
        overview_chars: int = DEFAULT_OVERVIEW_CHARS,
        include_sources: set[str] | None = None,
    ) -> None:
        self.field_dir = field_dir.expanduser().resolve()
        self.live_dir = live_dir.expanduser().resolve()
        self.entertainment_dir = entertainment_dir.expanduser().resolve() if entertainment_dir else None
        self.overview_chars = overview_chars
        self.include_sources = include_sources or set()

        self.live_dir.mkdir(parents=True, exist_ok=True)
        self.vectors_path = self.live_dir / "vectors.f32"
        self.norms_path = self.live_dir / "norms.f32"
        self.db_path = self.live_dir / "metadata.sqlite3"
        self.state_path = self.live_dir / "state.json"
        self.lock_path = self.live_dir / ".sync.lock"
        self.runtime_lock = threading.RLock()

        self._init_db()
        self.state = self._load_state()
        self.reconcile()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    row_id INTEGER PRIMARY KEY,
                    source TEXT NOT NULL,
                    source_id TEXT,
                    type TEXT,
                    title TEXT NOT NULL,
                    year TEXT,
                    external_url TEXT,
                    publisher TEXT,
                    availability TEXT,
                    genres_json TEXT,
                    overview TEXT,
                    image_url TEXT,
                    poster_path TEXT,
                    backdrop_path TEXT,
                    tmdb_id TEXT,
                    tmdb_kind TEXT,
                    extra_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS indexed_units (
                    unit_key TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    row_start INTEGER NOT NULL,
                    row_end INTEGER NOT NULL,
                    manifest_sha256 TEXT NOT NULL,
                    indexed_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _empty_state(self) -> dict[str, Any]:
        return {
            "schema_version": STATE_SCHEMA,
            "index_schema": INDEX_SCHEMA,
            "dim": DEFAULT_DIM,
            "use_freq": DEFAULT_USE_FREQ,
            "count": 0,
            "processed": {},
            "updated_at": utc_now(),
        }

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            state = self._empty_state()
            atomic_json(self.state_path, state)
            return state
        state = safe_json_load(self.state_path)
        if int(state.get("schema_version") or 0) != STATE_SCHEMA:
            raise RuntimeError(f"Unsupported state schema in {self.state_path}")
        state.setdefault("processed", {})
        state.setdefault("count", 0)
        state.setdefault("dim", DEFAULT_DIM)
        state.setdefault("use_freq", DEFAULT_USE_FREQ)
        return state

    def save_state(self) -> None:
        self.state["updated_at"] = utc_now()
        atomic_json(self.state_path, self.state)

    def db_connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.row_factory = sqlite3.Row
        return conn

    def _reconcile_locked(self) -> None:
        count = int(self.state.get("count") or 0)
        dim = int(self.state.get("dim") or DEFAULT_DIM)
        expected_vector_bytes = count * dim * 4
        expected_norm_bytes = count * 4

        self.vectors_path.touch(exist_ok=True)
        self.norms_path.touch(exist_ok=True)

        vector_bytes = self.vectors_path.stat().st_size
        norm_bytes = self.norms_path.stat().st_size

        if vector_bytes < expected_vector_bytes:
            raise RuntimeError(
                f"Live vectors are shorter than committed state: {vector_bytes} < {expected_vector_bytes}"
            )
        if norm_bytes < expected_norm_bytes:
            raise RuntimeError(
                f"Live norms are shorter than committed state: {norm_bytes} < {expected_norm_bytes}"
            )

        if vector_bytes > expected_vector_bytes:
            print(
                f"[live-index] repairing uncommitted vector tail: "
                f"{vector_bytes:,} -> {expected_vector_bytes:,} bytes"
            )
            with self.vectors_path.open("r+b") as fh:
                fh.truncate(expected_vector_bytes)
        if norm_bytes > expected_norm_bytes:
            print(
                f"[live-index] repairing uncommitted norm tail: "
                f"{norm_bytes:,} -> {expected_norm_bytes:,} bytes"
            )
            with self.norms_path.open("r+b") as fh:
                fh.truncate(expected_norm_bytes)

        conn = self.db_connect()
        try:
            conn.execute("DELETE FROM items WHERE row_id >= ?", (count,))
            conn.commit()
            db_count = int(conn.execute("SELECT COUNT(*) FROM items").fetchone()[0])
            if db_count != count:
                raise RuntimeError(
                    f"Live metadata count {db_count:,} does not match committed state {count:,}"
                )
        finally:
            conn.close()

    def reconcile(self) -> None:
        """Make file/SQLite tails agree with the last committed state."""
        with FileLock(self.lock_path), self.runtime_lock:
            self._reconcile_locked()

    def scan_units(self) -> list[SourceUnit]:
        units: list[SourceUnit] = []

        if self.entertainment_dir:
            manifest_path = self.entertainment_dir / "SUMMON_field_manifest.json"
            if manifest_path.exists() and (not self.include_sources or "entertainment" in self.include_sources):
                manifest = safe_json_load(manifest_path)
                count = int(manifest.get("count") or 0)
                dim = int(manifest.get("dim") or DEFAULT_DIM)
                use_freq = bool(manifest.get("use_freq", DEFAULT_USE_FREQ))
                metadata_files = manifest.get("metadata_files") or [
                    manifest.get("metadata_file") or "SUMMON_field_metadata.json"
                ]
                vectors_path = self.entertainment_dir / (
                    manifest.get("vectors_file") or "SUMMON_field_vectors.f32"
                )
                norms_path = self.entertainment_dir / (
                    manifest.get("norms_file") or "SUMMON_field_norms.f32"
                )
                units.append(
                    SourceUnit(
                        key=f"entertainment:{self.entertainment_dir}",
                        kind="entertainment",
                        manifest_path=manifest_path,
                        root_dir=self.entertainment_dir,
                        source="entertainment",
                        count=count,
                        dim=dim,
                        use_freq=use_freq,
                        vectors_path=vectors_path,
                        norms_path=norms_path,
                        metadata_paths=tuple(self.entertainment_dir / str(x) for x in metadata_files),
                        metadata_format="json-array",
                        manifest_sha256=sha256_file(manifest_path),
                        created_at=clean_text(manifest.get("created_at")),
                    )
                )

        shards_root = self.field_dir / "shards"
        if shards_root.exists():
            for manifest_path in sorted(shards_root.glob("*/shard-*/manifest.json")):
                if manifest_path.parent.name.endswith(".tmp"):
                    continue
                manifest = safe_json_load(manifest_path)
                source = clean_text(manifest.get("source")) or manifest_path.parent.parent.name
                # When the original SUMMON bundle is explicitly supplied, do not
                # index a second imported copy of the same entertainment field.
                if self.entertainment_dir and source == "entertainment":
                    continue
                if self.include_sources and source not in self.include_sources:
                    continue
                count = int(manifest.get("count") or 0)
                dim = int(manifest.get("dim") or DEFAULT_DIM)
                use_freq = bool(manifest.get("use_freq", DEFAULT_USE_FREQ))
                root = manifest_path.parent
                rel = manifest_path.relative_to(self.field_dir)
                units.append(
                    SourceUnit(
                        key=f"universal:{rel.as_posix()}",
                        kind="universal",
                        manifest_path=manifest_path,
                        root_dir=root,
                        source=source,
                        count=count,
                        dim=dim,
                        use_freq=use_freq,
                        vectors_path=root / (manifest.get("vectors_file") or "vectors.f32"),
                        norms_path=root / (manifest.get("norms_file") or "norms.f32"),
                        metadata_paths=(root / (manifest.get("metadata_file") or "metadata.jsonl.gz"),),
                        metadata_format="jsonl-gzip",
                        manifest_sha256=sha256_file(manifest_path),
                        created_at=clean_text(manifest.get("created_at")),
                    )
                )

        # Seed entertainment first, then all durable universal shards in stable order.
        units.sort(key=lambda u: (0 if u.kind == "entertainment" else 1, u.source, u.key))
        return units

    def pending_units(self) -> list[SourceUnit]:
        processed = self.state.get("processed") or {}
        out: list[SourceUnit] = []
        for unit in self.scan_units():
            previous = processed.get(unit.key)
            if previous:
                old_sha = clean_text(previous.get("manifest_sha256"))
                if old_sha and old_sha != unit.manifest_sha256:
                    raise RuntimeError(
                        f"Indexed unit changed after commit: {unit.manifest_path}. "
                        "Finalized shards must be immutable."
                    )
                continue
            out.append(unit)
        return out

    def validate_unit(self, unit: SourceUnit) -> None:
        if unit.count <= 0:
            raise ValueError(f"Invalid count in {unit.manifest_path}: {unit.count}")
        index_count = int(self.state.get("count") or 0)
        index_dim = int(self.state.get("dim") or DEFAULT_DIM)
        index_use_freq = bool(self.state.get("use_freq", DEFAULT_USE_FREQ))
        if index_count > 0 and unit.dim != index_dim:
            raise ValueError(f"Dimension mismatch: live={index_dim}, {unit.key}={unit.dim}")
        if index_count > 0 and unit.use_freq != index_use_freq:
            raise ValueError(
                f"use_freq mismatch: live={index_use_freq}, {unit.key}={unit.use_freq}"
            )
        expected_vectors = unit.count * unit.dim * 4
        expected_norms = unit.count * 4
        if not unit.vectors_path.exists():
            raise FileNotFoundError(unit.vectors_path)
        if not unit.norms_path.exists():
            raise FileNotFoundError(unit.norms_path)
        if unit.vectors_path.stat().st_size != expected_vectors:
            raise ValueError(
                f"Vector bytes mismatch for {unit.key}: "
                f"{unit.vectors_path.stat().st_size} != {expected_vectors}"
            )
        if unit.norms_path.stat().st_size != expected_norms:
            raise ValueError(
                f"Norm bytes mismatch for {unit.key}: "
                f"{unit.norms_path.stat().st_size} != {expected_norms}"
            )
        for path in unit.metadata_paths:
            if not path.exists():
                raise FileNotFoundError(path)

    def iter_metadata(self, unit: SourceUnit) -> Iterator[dict[str, Any]]:
        if unit.metadata_format == "jsonl-gzip":
            path = unit.metadata_paths[0]
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if isinstance(rec, dict):
                        yield rec
            return

        if unit.metadata_format == "json-array":
            for path in unit.metadata_paths:
                with path.open("r", encoding="utf-8") as fh:
                    rows = json.load(fh)
                if not isinstance(rows, list):
                    raise ValueError(f"Metadata is not a JSON array: {path}")
                for rec in rows:
                    if isinstance(rec, dict):
                        yield rec
            return

        raise ValueError(f"Unsupported metadata format: {unit.metadata_format}")

    def _overview_from_candidate(self, candidate_text: str, title: str) -> str:
        text = clean_text(candidate_text)
        for marker in ("Abstract:", "Description:"):
            if marker in text:
                return clamp(text.split(marker, 1)[1], self.overview_chars)
        if title and text.startswith(title):
            text = text[len(title):].lstrip(". ")
        return clamp(text, self.overview_chars)

    def display_row(self, rec: dict[str, Any], default_source: str) -> tuple[Any, ...]:
        metadata = rec.get("metadata") if isinstance(rec.get("metadata"), dict) else {}
        source = clean_text(rec.get("source")) or default_source
        source_id = clean_text(rec.get("source_id") or rec.get("sourceId"))
        record_type = clean_text(rec.get("type")) or ("paper" if source == "openalex" else "item")
        title = clean_text(rec.get("title")) or "Untitled"
        year = clean_text(rec.get("year"))
        external_url = clean_text(rec.get("external_url") or rec.get("externalUrl"))

        publisher = clean_text(rec.get("publisher") or metadata.get("publisher"))
        availability = clean_text(rec.get("availability") or metadata.get("availability"))

        genres = rec.get("genres")
        if not isinstance(genres, list):
            genres = metadata.get("genres")
        if not isinstance(genres, list) or not genres:
            genres = metadata.get("topics") or metadata.get("keywords") or []
        genres = [clean_text(x) for x in genres if clean_text(x)][:8]

        if not publisher and source == "openalex":
            authors = metadata.get("authors") or []
            if isinstance(authors, list):
                publisher = ", ".join(clean_text(x) for x in authors[:3] if clean_text(x))

        overview = clean_text(rec.get("overview"))
        if not overview:
            overview = self._overview_from_candidate(clean_text(rec.get("candidate_text")), title)
        else:
            overview = clamp(overview, self.overview_chars)

        # SUMMON_UNIVERSAL_IMAGE_ALIASES_V27_1
        # Preserve artwork emitted by every connector schema, not only the
        # original camelCase entertainment schema.
        image_url = clean_text(
            rec.get("imageUrl")
            or rec.get("image_url")
            or rec.get("image")
            or rec.get("coverUrl")
            or rec.get("cover_url")
            or rec.get("cover")
            or rec.get("thumbnailUrl")
            or rec.get("thumbnail_url")
            or rec.get("thumbnail")
            or rec.get("src")
            or metadata.get("imageUrl")
            or metadata.get("image_url")
            or metadata.get("image")
            or metadata.get("coverUrl")
            or metadata.get("cover_url")
            or metadata.get("cover")
            or metadata.get("thumbnailUrl")
            or metadata.get("thumbnail_url")
            or metadata.get("thumbnail")
            or metadata.get("src")
        )
        poster_path = clean_text(
            rec.get("posterPath")
            or rec.get("poster_path")
            or metadata.get("posterPath")
            or metadata.get("poster_path")
            or image_url
        )
        backdrop_path = clean_text(
            rec.get("backdropPath")
            or rec.get("backdrop_path")
            or metadata.get("backdropPath")
            or metadata.get("backdrop_path")
            or image_url
        )
        tmdb_id = clean_text(rec.get("tmdbId") or metadata.get("tmdbId"))
        tmdb_kind = clean_text(rec.get("tmdbKind") or metadata.get("tmdbKind"))

        extra = {
            "doi": metadata.get("doi"),
            "openalex_id": metadata.get("openalex_id"),
            "cited_by_count": metadata.get("cited_by_count"),
            "language": metadata.get("language"),
            "is_retracted": metadata.get("is_retracted"),
            "steamAppId": rec.get("steamAppId") or metadata.get("steamAppId"),
        }
        extra = {k: v for k, v in extra.items() if v not in (None, "", [], {})}

        return (
            source,
            source_id,
            record_type,
            title,
            year,
            external_url,
            publisher,
            availability,
            json.dumps(genres, ensure_ascii=False, separators=(",", ":")),
            overview,
            image_url,
            poster_path,
            backdrop_path,
            tmdb_id,
            tmdb_kind,
            json.dumps(extra, ensure_ascii=False, separators=(",", ":")),
        )

    @staticmethod
    def append_file(src: Path, dst_handle) -> None:
        with src.open("rb") as source:
            shutil.copyfileobj(source, dst_handle, length=16 * 1024 * 1024)

    def add_unit(self, unit: SourceUnit) -> None:
        self.validate_unit(unit)
        with FileLock(self.lock_path), self.runtime_lock:
            # Re-read state in case another explicit sync process advanced it.
            self.state = self._load_state()
            if unit.key in (self.state.get("processed") or {}):
                return
            self._reconcile_locked()

            row_start = int(self.state.get("count") or 0)
            dim = int(self.state.get("dim") or unit.dim)
            if row_start == 0:
                dim = unit.dim
                self.state["dim"] = unit.dim
                self.state["use_freq"] = unit.use_freq

            vector_start = self.vectors_path.stat().st_size
            norm_start = self.norms_path.stat().st_size
            inserted = 0
            conn = self.db_connect()
            try:
                print(
                    f"[live-index] adding {unit.source} · {unit.manifest_path.parent.name} · "
                    f"{human_count(unit.count)} records"
                )

                with self.vectors_path.open("ab") as vf, self.norms_path.open("ab") as nf:
                    self.append_file(unit.vectors_path, vf)
                    self.append_file(unit.norms_path, nf)
                    vf.flush(); os.fsync(vf.fileno())
                    nf.flush(); os.fsync(nf.fileno())

                conn.execute("BEGIN IMMEDIATE")
                sql = (
                    "INSERT INTO items ("
                    "row_id,source,source_id,type,title,year,external_url,publisher,availability,"
                    "genres_json,overview,image_url,poster_path,backdrop_path,tmdb_id,tmdb_kind,extra_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                )
                batch: list[tuple[Any, ...]] = []
                for rec in self.iter_metadata(unit):
                    display = self.display_row(rec, unit.source)
                    batch.append((row_start + inserted, *display))
                    inserted += 1
                    if len(batch) >= 1000:
                        conn.executemany(sql, batch)
                        batch.clear()
                if batch:
                    conn.executemany(sql, batch)

                if inserted != unit.count:
                    raise RuntimeError(
                        f"Metadata row count mismatch for {unit.key}: {inserted} != {unit.count}"
                    )

                row_end = row_start + inserted
                conn.execute(
                    "INSERT INTO indexed_units "
                    "(unit_key,source,count,row_start,row_end,manifest_sha256,indexed_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        unit.key,
                        unit.source,
                        unit.count,
                        row_start,
                        row_end,
                        unit.manifest_sha256,
                        utc_now(),
                    ),
                )
                conn.commit()

                processed = self.state.setdefault("processed", {})
                processed[unit.key] = {
                    "source": unit.source,
                    "count": unit.count,
                    "row_start": row_start,
                    "row_end": row_end,
                    "manifest": str(unit.manifest_path),
                    "manifest_sha256": unit.manifest_sha256,
                    "indexed_at": utc_now(),
                }
                self.state["count"] = row_end
                self.state["dim"] = dim
                self.state["use_freq"] = unit.use_freq
                self.save_state()
                print(
                    f"[live-index] ready · {human_count(row_end)} total objects · "
                    f"{len(processed):,} units"
                )
            except Exception:
                with contextlib.suppress(Exception):
                    conn.rollback()
                with self.vectors_path.open("r+b") as vf:
                    vf.truncate(vector_start)
                with self.norms_path.open("r+b") as nf:
                    nf.truncate(norm_start)
                with contextlib.suppress(Exception):
                    conn.execute("DELETE FROM items WHERE row_id >= ?", (row_start,))
                    conn.execute("DELETE FROM indexed_units WHERE unit_key = ?", (unit.key,))
                    conn.commit()
                raise
            finally:
                conn.close()

    def sync(self, max_new: int | None = None) -> int:
        added = 0
        for unit in self.pending_units():
            self.add_unit(unit)
            added += 1
            if max_new is not None and added >= max_new:
                break
        return added

    def manifest(self) -> dict[str, Any]:
        with self.runtime_lock:
            self.state = self._load_state()
            processed = self.state.get("processed") or {}
            sources: dict[str, dict[str, int]] = {}
            for info in processed.values():
                source = clean_text(info.get("source")) or "unknown"
                bucket = sources.setdefault(source, {"count": 0, "units": 0})
                bucket["count"] += int(info.get("count") or 0)
                bucket["units"] += 1
            return {
                "schema_version": INDEX_SCHEMA,
                "count": int(self.state.get("count") or 0),
                "dim": int(self.state.get("dim") or DEFAULT_DIM),
                "use_freq": bool(self.state.get("use_freq", DEFAULT_USE_FREQ)),
                "units": len(processed),
                "sources": sources,
                "updated_at": self.state.get("updated_at"),
            }

    # SUMMON_CONTINUOUS_RESULT_PAGING_V26
    # SUMMON_HAPPY_58MS_RUNTIME_0c4e3c93f5
    def _search_vector_happy(self, vector: Iterable[float], k: int, chunk_rows: int) -> list[dict[str, Any]]:
        info = self.manifest()
        count = int(info["count"])
        dim = int(info["dim"])
        if count <= 0:
            return []

        q = np.asarray(list(vector), dtype=np.float32)
        if q.ndim != 1 or q.shape[0] != dim:
            raise ValueError(f"Query vector shape {q.shape}; expected ({dim},)")
        q_norm = float(np.linalg.norm(q))
        if not math.isfinite(q_norm) or q_norm <= 0:
            raise ValueError("Query vector has zero or invalid norm")

        k = max(1, min(int(k), 10_000))
        chunk_rows = max(10_000, int(chunk_rows))

        vectors = np.memmap(self.vectors_path, mode="r", dtype="<f4", shape=(count, dim))
        norms = np.memmap(self.norms_path, mode="r", dtype="<f4", shape=(count,))

        best_scores = np.empty((0,), dtype=np.float32)
        best_ids = np.empty((0,), dtype=np.int64)

        for start in range(0, count, chunk_rows):
            end = min(count, start + chunk_rows)
            block = vectors[start:end]
            block_norms = norms[start:end]
            dots = block @ q
            denom = block_norms * q_norm
            scores = np.divide(
                dots,
                denom,
                out=np.full_like(dots, -np.inf, dtype=np.float32),
                where=denom > 0,
            )

            local_k = min(k, scores.shape[0])
            if local_k <= 0:
                continue
            if local_k == scores.shape[0]:
                local_idx = np.arange(scores.shape[0])
            else:
                local_idx = np.argpartition(scores, -local_k)[-local_k:]
            local_scores = scores[local_idx]
            local_ids = local_idx.astype(np.int64) + start

            if best_scores.size:
                merged_scores = np.concatenate((best_scores, local_scores))
                merged_ids = np.concatenate((best_ids, local_ids))
            else:
                merged_scores = local_scores
                merged_ids = local_ids

            keep = min(k, merged_scores.shape[0])
            if keep == merged_scores.shape[0]:
                picked = np.arange(merged_scores.shape[0])
            else:
                picked = np.argpartition(merged_scores, -keep)[-keep:]
            order = picked[np.argsort(merged_scores[picked])[::-1]]
            best_scores = merged_scores[order]
            best_ids = merged_ids[order]

        row_ids = [int(x) for x in best_ids.tolist()]
        if not row_ids:
            return []

        conn = self.db_connect()
        try:
            placeholders = ",".join("?" for _ in row_ids)
            rows = conn.execute(
                f"SELECT * FROM items WHERE row_id IN ({placeholders})",
                row_ids,
            ).fetchall()
        finally:
            conn.close()

        by_id = {int(row["row_id"]): row for row in rows}
        results: list[dict[str, Any]] = []
        for row_id, score in zip(row_ids, best_scores.tolist()):
            row = by_id.get(row_id)
            if row is None:
                continue
            try:
                genres = json.loads(row["genres_json"] or "[]")
            except Exception:
                genres = []
            try:
                extra = json.loads(row["extra_json"] or "{}")
            except Exception:
                extra = {}
            item = {
                "rowId": row_id,
                "source": row["source"],
                "sourceId": row["source_id"],
                "type": row["type"],
                "title": row["title"],
                "year": row["year"],
                "externalUrl": row["external_url"],
                "publisher": row["publisher"],
                "availability": row["availability"],
                "genres": genres,
                "overview": row["overview"],
                "imageUrl": row["image_url"],
                "posterPath": row["poster_path"],
                "backdropPath": row["backdrop_path"],
                "tmdbId": row["tmdb_id"],
                "tmdbKind": row["tmdb_kind"],
                "score": float(score),
            }
            item.update(extra)
            results.append({k: v for k, v in item.items() if v not in (None, "", [], {})})
        return results

    def search_vector(
        self,
        vector: Iterable[float],
        k: int,
        chunk_rows: int,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        # Thin compatibility wrapper around the proven July 16 runtime.
        page_k = max(1, min(int(k), 100))
        offset = max(0, min(int(offset), 9_990))
        target_k = offset + page_k
        rows = self._search_vector_happy(vector, k=target_k, chunk_rows=chunk_rows)
        return rows[offset:offset + page_k]


class LiveFieldApplication:
    def __init__(self, args: argparse.Namespace, index: LiveFieldIndex):
        self.args = args
        self.index = index
        self.stop_event = threading.Event()
        self.sync_thread: threading.Thread | None = None

    def start_sync_thread(self) -> None:
        def run() -> None:
            while not self.stop_event.is_set():
                try:
                    added = self.index.sync(max_new=1)
                    if added:
                        continue
                except Exception:
                    print("[live-index] background sync failed", file=sys.stderr)
                    traceback.print_exc()
                self.stop_event.wait(self.args.sync_interval)

        self.sync_thread = threading.Thread(target=run, name="live-field-sync", daemon=True)
        self.sync_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.sync_thread:
            self.sync_thread.join(timeout=3)


class Handler(BaseHTTPRequestHandler):
    server_version = "ARBITERLiveField/1.0"

    @property
    def app(self) -> LiveFieldApplication:
        return self.server.app  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stdout.write(f"[http] {self.address_string()} · {fmt % args}\n")

    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", self.app.args.cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept")
        self.send_header("Access-Control-Max-Age", "86400")

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/health", "/field/health"):
            self.send_json(200, {"ok": True, **self.app.index.manifest()})
            return
        if path in ("/v1/manifest", "/field/v1/manifest"):
            self.send_json(200, self.app.index.manifest())
            return
        self.serve_static(path)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path not in ("/v1/search", "/field/v1/search"):
            self.send_json(404, {"error": "not found"})
            return

        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > self.app.args.max_request_bytes:
                raise ValueError("Invalid request body length")
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError("Request body must be a JSON object")

            k = int(payload.get("k") or 10)
            offset = int(payload.get("offset") or 0)
            vector = payload.get("vector")
            text = clean_text(payload.get("text"))

            try:
                (self.app.index.live_dir / ".last_search").touch()
            except OSError:
                pass

            if vector is None:
                if not text:
                    raise ValueError("Provide text or vector")
                info = self.app.index.manifest()
                embedded = post_json(
                    self.app.args.embed_url,
                    {"texts": [text], "use_freq": bool(info["use_freq"])},
                    timeout=self.app.args.embed_timeout,
                )
                rows = extract_vectors(embedded)
                if not rows:
                    raise RuntimeError("Embed server returned no vector")
                vector = rows[0]

            started = time.perf_counter()
            results = self.app.index.search_vector(
                vector,
                k=k,
                chunk_rows=self.app.args.search_chunk_rows,
                offset=offset,
            )
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            info = self.app.index.manifest()
            self.send_json(
                200,
                {
                    "query": text or None,
                    "count": info["count"],
                    "dim": info["dim"],
                    "units": info["units"],
                    "sources": info["sources"],
                    "updated_at": info["updated_at"],
                    "elapsed_ms": elapsed_ms,
                    "offset": offset,
                    "page_size": len(results),
                    "results": results,
                },
            )
        except ValueError as exc:
            self.send_json(400, {"error": str(exc)})
        except Exception as exc:
            traceback.print_exc()
            self.send_json(500, {"error": str(exc)})

    def serve_static(self, request_path: str) -> None:
        args = self.app.args
        if request_path in ("", "/") and args.html:
            path = Path(args.html).expanduser().resolve()
        else:
            if not args.assets_dir:
                self.send_json(404, {"error": "not found"})
                return
            root = Path(args.assets_dir).expanduser().resolve()
            rel = unquote(request_path).lstrip("/")
            candidate = (root / rel).resolve()
            try:
                candidate.relative_to(root)
            except ValueError:
                self.send_json(403, {"error": "forbidden"})
                return
            path = candidate
            if path.is_dir():
                path = path / "index.html"

        if not path.exists() or not path.is_file():
            self.send_json(404, {"error": "not found"})
            return

        ctype, encoding = mimetypes.guess_type(str(path))
        ctype = ctype or "application/octet-stream"
        size = path.stat().st_size
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", ctype)
        if encoding:
            self.send_header("Content-Encoding", encoding)
        self.send_header("Content-Length", str(size))
        self.send_header("Cache-Control", "no-cache" if path.suffix in {".html", ".json"} else "public, max-age=3600")
        self.end_headers()
        with path.open("rb") as fh:
            shutil.copyfileobj(fh, self.wfile, length=1024 * 1024)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--field-dir",
        type=Path,
        default=Path("~/ARBITER_UNIVERSAL_FIELD").expanduser(),
        help="Universal field root produced by ARBITER_field_forge.py",
    )
    parser.add_argument(
        "--live-dir",
        type=Path,
        default=None,
        help="Growing live index directory (default: <field-dir>/live_index)",
    )
    parser.add_argument(
        "--entertainment-dir",
        type=Path,
        default=None,
        help="Optional existing SUMMON pre-embedded bundle to seed into the same index",
    )
    parser.add_argument(
        "--include-source",
        action="append",
        default=[],
        help="Only index this source; repeat for multiple sources",
    )
    parser.add_argument(
        "--overview-chars",
        type=int,
        default=DEFAULT_OVERVIEW_CHARS,
        help="Display synopsis characters kept in SQLite; embeddings remain untouched",
    )


def make_index(args: argparse.Namespace) -> LiveFieldIndex:
    live_dir = args.live_dir or (args.field_dir.expanduser() / "live_index")
    return LiveFieldIndex(
        field_dir=args.field_dir,
        live_dir=live_dir,
        entertainment_dir=args.entertainment_dir,
        overview_chars=args.overview_chars,
        include_sources=set(args.include_source or []),
    )


def cmd_status(args: argparse.Namespace) -> int:
    index = make_index(args)
    info = index.manifest()
    pending = index.pending_units()
    print(json.dumps({**info, "pending_units": len(pending)}, indent=2))
    if pending:
        print("\nNext units:")
        for unit in pending[:10]:
            print(f"  {unit.source:14s} {unit.manifest_path.parent.name:16s} {unit.count:>10,}")
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    index = make_index(args)
    added = index.sync(max_new=args.max_new)
    info = index.manifest()
    print(
        f"[live-index] sync complete · added {added:,} units · "
        f"{human_count(info['count'])} searchable objects"
    )
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    index = make_index(args)

    # Bind immediately. Finalized units join from the background thread so a
    # large first import never blocks Render's port/health detection.
    app = LiveFieldApplication(args, index)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    server.app = app  # type: ignore[attr-defined]
    # SUMMON_SEARCH_SYNC_SEPARATION_20260720T035235Z
    # Search stays isolated from shard ingestion. External live-sync owns writes.

    info = index.manifest()
    print(
        f"[live-index] serving http://{args.host}:{args.port} · "
        f"{human_count(info['count'])} objects ready · new shards auto-join"
    )
    if args.html:
        print(f"[live-index] SUMMON: http://{args.host}:{args.port}/")
    print(f"[live-index] search: http://{args.host}:{args.port}/field/v1/search")

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\n[live-index] stopping")
    finally:
        app.stop()
        server.server_close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Continuously searchable ARBITER universal field")
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="Show indexed and pending field state")
    add_common_args(p_status)
    p_status.set_defaults(func=cmd_status)

    p_sync = sub.add_parser("sync", help="Append all currently finalized shards to the live index")
    add_common_args(p_sync)
    p_sync.add_argument("--max-new", type=int, default=None, help="Stop after this many new units")
    p_sync.set_defaults(func=cmd_sync)

    p_serve = sub.add_parser("serve", help="Serve SUMMON and hot-add finalized shards")
    add_common_args(p_serve)
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8787)
    p_serve.add_argument("--embed-url", default="http://127.0.0.1:8000/v1/embed")
    p_serve.add_argument("--embed-timeout", type=int, default=120)
    p_serve.add_argument("--sync-interval", type=float, default=20.0)
    p_serve.add_argument("--search-chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS)
    p_serve.add_argument("--cors-origin", default="*")
    p_serve.add_argument("--max-request-bytes", type=int, default=2 * 1024 * 1024)
    p_serve.add_argument("--html", type=Path, default=None, help="HTML file served at /")
    p_serve.add_argument("--assets-dir", type=Path, default=None, help="Directory serving SUMMON field assets")
    p_serve.set_defaults(func=cmd_serve)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
