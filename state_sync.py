"""
State sync — pulls/pushes the persistent state files to/from a hidden Drive folder.

Why: GitHub Actions runners are ephemeral. Each run starts with empty disk.
For the delta logic (and per-course "new institute" detection) to work, the
manifest.db and last_state.json files must survive between runs.

We stash them in:  MCC Archive / _state / { manifest.db, PG_last_state.json, UG_last_state.json }

Public API:
    pull_state(cfg, drive, log)   # call once at start of run_all.py
    push_state(cfg, drive, log)   # call once at end (in a finally block)

Safe to use locally too — it just keeps your local files in sync with Drive.
If the _state folder doesn't exist on Drive yet (first run), pull_state is a no-op.
"""
from __future__ import annotations

import io
import logging
from pathlib import Path

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


STATE_FOLDER_NAME = "_state"

# Map: filename on Drive -> local path resolver (cfg -> Path)
_STATE_FILES: list[tuple[str, callable]] = [
    ("manifest.db", lambda cfg: Path(cfg["paths"]["manifest"]).resolve()),
    ("PG_last_state.json",
        lambda cfg: Path(cfg["paths"].get("admissions_staging",
                                          "C:/MCC-Admissions-Staging")) / "PG" / "last_state.json"),
    ("UG_last_state.json",
        lambda cfg: Path(cfg["paths"].get("admissions_staging",
                                          "C:/MCC-Admissions-Staging")) / "UG" / "last_state.json"),
]


def _state_folder_id(drive, log: logging.Logger) -> str:
    """Get (or create) the _state folder ID under the configured Drive root."""
    return drive._ensure_folder((STATE_FOLDER_NAME,))


def _find_file_in_folder(drive, folder_id: str, name: str) -> str | None:
    res = drive.svc.files().list(
        q=f"'{folder_id}' in parents and name='{name}' and trashed=false",
        fields="files(id,name,modifiedTime,size)",
        pageSize=1,
    ).execute().get("files", [])
    return res[0]["id"] if res else None


def pull_state(cfg: dict, drive, log: logging.Logger) -> None:
    """Download every state file from Drive into its local path. Silently
    skips files that don't yet exist on Drive (first run)."""
    folder_id = _state_folder_id(drive, log)
    log.info(f"[state] pulling state from Drive _state folder ({folder_id}) ...")
    for fname, local_resolver in _STATE_FILES:
        local_path = local_resolver(cfg)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        file_id = _find_file_in_folder(drive, folder_id, fname)
        if not file_id:
            log.info(f"  [state] {fname}: not present on Drive yet (skipping)")
            continue
        try:
            req = drive.svc.files().get_media(fileId=file_id)
            buf = io.FileIO(local_path, "wb")
            downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            buf.close()
            size = local_path.stat().st_size
            log.info(f"  [state] {fname}: pulled ({size:,} bytes -> {local_path})")
        except Exception as e:
            log.warning(f"  [state] {fname}: pull failed: {e}")


def push_state(cfg: dict, drive, log: logging.Logger) -> None:
    """Upload every state file from its local path to the Drive _state folder.
    Replaces existing copies in place. Silently skips local files that don't exist."""
    folder_id = _state_folder_id(drive, log)
    log.info(f"[state] pushing state to Drive _state folder ({folder_id}) ...")
    for fname, local_resolver in _STATE_FILES:
        local_path = local_resolver(cfg)
        if not local_path.exists():
            log.info(f"  [state] {fname}: local file missing (skipping push)")
            continue
        try:
            mime = "application/octet-stream"
            if fname.endswith(".json"):
                mime = "application/json"
            elif fname.endswith(".db"):
                mime = "application/x-sqlite3"
            media = MediaFileUpload(str(local_path), mimetype=mime, resumable=True)
            existing_id = _find_file_in_folder(drive, folder_id, fname)
            if existing_id:
                drive.svc.files().update(
                    fileId=existing_id, media_body=media,
                    body={"name": fname}, fields="id",
                ).execute()
                log.info(f"  [state] {fname}: updated on Drive")
            else:
                drive.svc.files().create(
                    media_body=media,
                    body={"name": fname, "parents": [folder_id]},
                    fields="id",
                ).execute()
                log.info(f"  [state] {fname}: created on Drive")
        except Exception as e:
            log.warning(f"  [state] {fname}: push failed: {e}")
