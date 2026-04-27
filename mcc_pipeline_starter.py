"""
MCC PDF Mirror — single-file starter script.

Stages implemented:
  1. Crawler (BFS over mcc.nic.in, Playwright fallback for JS menus)
  2. Link resolver (follows redirects, sniffs Content-Type / magic bytes)
  3. Breadcrumb + heading tagger
  4. Downloader (retry, SHA-256, PDF validation)
  5. SQLite manifest
  6. Drive uploader (service-account auth, resumable upload, revision-on-update)

Usage:
    python mcc_pipeline_starter.py --mode crawl-only   # stages 1-5, no Drive
    python mcc_pipeline_starter.py --mode full         # stages 1-6, first bulk
    python mcc_pipeline_starter.py --mode delta        # stages 1-6, only new/changed

Config:
    config.yaml next to this script.

Credentials:
    credentials/service-account.json (only needed for --mode full / delta)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.robotparser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
import yaml
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

# Drive imports are lazy — only needed for --mode full / delta
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaFileUpload
# from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Config / logging
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent.resolve()


def load_config() -> dict:
    with open(SCRIPT_DIR / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(logs_dir: Path) -> logging.Logger:
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file = logs_dir / f"run-{ts}.log"
    logger = logging.getLogger("mcc")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    sh = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for h in (fh, sh):
        h.setFormatter(fmt)
        logger.addHandler(h)
    logger.info(f"log file: {log_file}")
    return logger


# ---------------------------------------------------------------------------
# Title / breadcrumb helpers
# ---------------------------------------------------------------------------

# Strings (case-insensitive) that mean "this is just the site title, drop it"
SITE_TITLE_NOISE = (
    "medical counselling committee",
    "mcc",
    "| india",
    "- india",
)

# Max breadcrumb depth we ever include — keeps Drive folders sane
MAX_BREADCRUMB_DEPTH = 4


def clean_page_title(raw: str) -> str:
    """MCC titles look like 'UG Medical Counselling | Medical Counselling Committee (MCC) | India'.
    Keep only the first, page-specific segment. Return '' for pages whose title
    is just the site-wide title (e.g. the homepage) so they don't pollute the breadcrumb.
    """
    if not raw:
        return ""
    for sep in (" | ", " — ", " – ", " - "):
        if sep in raw:
            first = raw.split(sep, 1)[0].strip()
            if first and not any(tok in first.lower() for tok in SITE_TITLE_NOISE):
                return first
            return ""  # first segment is the site name — treat page as rootish
    if any(tok in raw.lower() for tok in SITE_TITLE_NOISE):
        return ""
    return raw.strip()


def _extend_breadcrumb(existing: list[str], next_title: str) -> list[str]:
    """Append next_title unless it equals the last crumb (prevents A>A>A loops).
    Cap depth at MAX_BREADCRUMB_DEPTH.
    """
    if not next_title:
        return existing[:MAX_BREADCRUMB_DEPTH]
    if existing and existing[-1].lower() == next_title.lower():
        return existing[:MAX_BREADCRUMB_DEPTH]
    return (existing + [next_title])[:MAX_BREADCRUMB_DEPTH]


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class PdfRecord:
    url: str
    breadcrumb: list[str]
    heading: str
    link_text: str
    sha256: str = ""
    size_bytes: int = 0
    last_modified: str = ""
    local_path: str = ""
    status: str = "pending"
    drive_file_id: str = ""


@dataclass
class PageNode:
    url: str
    title: str
    breadcrumb: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Manifest (SQLite)
# ---------------------------------------------------------------------------


class Manifest:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.con = sqlite3.connect(db_path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self):
        self.con.executescript(
            """
            CREATE TABLE IF NOT EXISTS pdfs (
              url            TEXT PRIMARY KEY,
              sha256         TEXT,
              size_bytes     INTEGER,
              last_modified  TEXT,
              drive_file_id  TEXT,
              drive_path     TEXT,
              drive_filename TEXT,
              breadcrumb     TEXT,
              heading        TEXT,
              link_text      TEXT,
              first_seen     TEXT,
              last_checked   TEXT,
              status         TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pdfs_sha ON pdfs(sha256);
            CREATE TABLE IF NOT EXISTS runs (
              run_id         TEXT PRIMARY KEY,
              started_at     TEXT,
              finished_at    TEXT,
              mode           TEXT,
              new_count      INTEGER,
              updated_count  INTEGER,
              skipped_count  INTEGER,
              failed_count   INTEGER
            );
            """
        )
        self.con.commit()

    def get(self, url: str) -> dict | None:
        cur = self.con.execute("SELECT * FROM pdfs WHERE url = ?", (url,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

    def upsert(self, rec: PdfRecord, drive_path: str, drive_filename: str):
        now = datetime.now(timezone.utc).isoformat()
        existing = self.get(rec.url)
        first_seen = existing["first_seen"] if existing else now
        self.con.execute(
            """INSERT INTO pdfs(url, sha256, size_bytes, last_modified,
                    drive_file_id, drive_path, drive_filename, breadcrumb,
                    heading, link_text, first_seen, last_checked, status)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET
                    sha256=excluded.sha256,
                    size_bytes=excluded.size_bytes,
                    last_modified=excluded.last_modified,
                    drive_file_id=COALESCE(excluded.drive_file_id, pdfs.drive_file_id),
                    drive_path=excluded.drive_path,
                    drive_filename=excluded.drive_filename,
                    breadcrumb=excluded.breadcrumb,
                    heading=excluded.heading,
                    link_text=excluded.link_text,
                    last_checked=excluded.last_checked,
                    status=excluded.status
            """,
            (
                rec.url, rec.sha256, rec.size_bytes, rec.last_modified,
                rec.drive_file_id or None, drive_path, drive_filename,
                json.dumps(rec.breadcrumb, ensure_ascii=False),
                rec.heading, rec.link_text, first_seen, now, rec.status,
            ),
        )
        self.con.commit()

    def record_run(self, run_id, mode, started, finished, new, updated, skipped, failed):
        self.con.execute(
            """INSERT INTO runs VALUES(?,?,?,?,?,?,?,?)""",
            (run_id, started, finished, mode, new, updated, skipped, failed),
        )
        self.con.commit()


# ---------------------------------------------------------------------------
# Crawler + resolver
# ---------------------------------------------------------------------------


class Crawler:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger
        self.session = requests.Session()
        self.session.headers["User-Agent"] = cfg["crawler"]["user_agent"]
        self.domain = cfg["domain"]
        self.delay = cfg["crawler"]["polite_delay_seconds"]
        self.max_pages = cfg["crawler"]["max_pages"]
        self.robots = self._load_robots()
        self._playwright = None  # lazy

    def _load_robots(self):
        if not self.cfg["crawler"].get("respect_robots_txt", True):
            return None
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"https://{self.domain}/robots.txt")
        try:
            rp.read()
        except Exception as e:
            self.log.warning(f"robots.txt read failed: {e}")
        return rp

    def _allowed(self, url: str) -> bool:
        if self.robots is None:
            return True
        return self.robots.can_fetch(self.cfg["crawler"]["user_agent"], url)

    @staticmethod
    def _same_domain(url: str, domain: str) -> bool:
        try:
            host = urllib.parse.urlparse(url).hostname or ""
            return host == domain or host.endswith("." + domain)
        except Exception:
            return False

    @staticmethod
    def _normalize(url: str) -> str:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        q.sort()
        return urllib.parse.urlunparse(
            (p.scheme.lower(), (p.hostname or "").lower() + (f":{p.port}" if p.port else ""),
             p.path or "/", "", urllib.parse.urlencode(q), "")
        )

    def _fetch_html(self, url: str) -> tuple[str, str]:
        """Return (html, final_url). Falls back to Playwright if static yields no links."""
        try:
            r = self.session.get(url, timeout=30, allow_redirects=True)
            if r.status_code != 200:
                self.log.info(f"[http {r.status_code}] {url}")
                return "", r.url
            ct = r.headers.get("Content-Type", "")
            if "html" not in ct.lower():
                return "", r.url
            # Heuristic: if <a> count is suspiciously low, re-render with Playwright.
            if r.text.count("<a ") < 5:
                rendered = self._render_with_playwright(url)
                if rendered:
                    return rendered, url
            return r.text, r.url
        except requests.RequestException as e:
            self.log.warning(f"[fetch fail] {url}: {e}")
            return "", url

    def _render_with_playwright(self, url: str) -> str:
        try:
            if self._playwright is None:
                from playwright.sync_api import sync_playwright
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch(headless=True)
            page = self._browser.new_page(user_agent=self.cfg["crawler"]["user_agent"])
            page.goto(url, wait_until="networkidle", timeout=30000)
            html = page.content()
            page.close()
            self.log.info(f"[playwright] rendered {url}")
            return html
        except Exception as e:
            self.log.warning(f"[playwright fail] {url}: {e}")
            return ""

    def crawl(self) -> list[PdfRecord]:
        seed = self.cfg["seed_url"]
        visited: set[str] = set()
        queue: list[PageNode] = [PageNode(seed, "Home", [])]
        pdf_records: dict[str, PdfRecord] = {}
        pages_done = 0

        while queue and pages_done < self.max_pages:
            node = queue.pop(0)
            nurl = self._normalize(node.url)
            if nurl in visited:
                continue
            visited.add(nurl)
            if not self._same_domain(node.url, self.domain):
                continue
            if not self._allowed(node.url):
                self.log.info(f"[robots block] {node.url}")
                continue

            html, final_url = self._fetch_html(node.url)
            pages_done += 1
            time.sleep(self.delay)
            if not html:
                continue

            soup = BeautifulSoup(html, "lxml")
            raw_title = (soup.title.string or node.title).strip() if soup.title else node.title
            page_title = clean_page_title(raw_title)
            breadcrumb = _extend_breadcrumb(node.breadcrumb, page_title)

            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if not href or href.startswith("#") or href.lower().startswith("javascript:"):
                    continue
                abs_url = urllib.parse.urljoin(final_url, href)
                nabs = self._normalize(abs_url)

                link_text = a.get_text(" ", strip=True) or "(no text)"
                heading = self._nearest_heading(a)

                if self._looks_like_pdf(abs_url):
                    resolved = self._resolve_pdf(abs_url)
                    if resolved and resolved not in pdf_records:
                        pdf_records[resolved] = PdfRecord(
                            url=resolved,
                            breadcrumb=breadcrumb,
                            heading=heading or page_title,
                            link_text=link_text[:300],
                        )
                        self.log.info(f"[pdf] {resolved}  ← {' > '.join(breadcrumb)}")
                elif self._same_domain(abs_url, self.domain) and nabs not in visited:
                    queue.append(PageNode(abs_url, link_text or page_title, breadcrumb))

        self.log.info(f"crawl done. pages={pages_done} pdfs={len(pdf_records)}")
        self._shutdown_playwright()
        return list(pdf_records.values())

    def _shutdown_playwright(self):
        if self._playwright:
            try:
                self._browser.close()
                self._playwright.stop()
            except Exception:
                pass

    @staticmethod
    def _nearest_heading(anchor) -> str:
        """Find the nearest preceding h1/h2/h3 by walking up/back through the tree."""
        for prev in anchor.find_all_previous(["h1", "h2", "h3", "h4"]):
            text = prev.get_text(" ", strip=True)
            if text:
                return text[:200]
        return ""

    def _looks_like_pdf(self, url: str) -> bool:
        low = url.lower()
        if low.endswith(".pdf"):
            return True
        # Classic redirector patterns used by gov sites
        if any(tok in low for tok in ("showpdf", "download.aspx", "viewpdf", "getfile", "/pdf/")):
            return True
        return False

    def _resolve_pdf(self, url: str) -> str | None:
        """HEAD-then-GET probe. Returns final URL if payload is a PDF, else None."""
        try:
            r = self.session.head(url, allow_redirects=True, timeout=20)
            ct = r.headers.get("Content-Type", "").lower()
            if "pdf" in ct:
                return r.url
            # Some servers return 200 HTML to HEAD then serve the PDF on GET — fall through
            r = self.session.get(url, stream=True, allow_redirects=True, timeout=20)
            ct = r.headers.get("Content-Type", "").lower()
            chunk = next(r.iter_content(5), b"")
            r.close()
            if "pdf" in ct or chunk == b"%PDF-":
                return r.url
        except requests.RequestException as e:
            self.log.warning(f"[resolve fail] {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------


def sanitize_name(s: str, max_len: int = 120) -> str:
    s = re.sub(r"[\\/:*?\"<>|\r\n\t]+", " ", s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s[:max_len].rstrip(" .") or "untitled"


def sanitize_path_component(s: str) -> str:
    return sanitize_name(s, 80)


class Downloader:
    def __init__(self, cfg: dict, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger
        self.session = requests.Session()
        self.session.headers["User-Agent"] = cfg["crawler"]["user_agent"]
        self.staging = Path(cfg["paths"]["staging"]).resolve()
        self.staging.mkdir(parents=True, exist_ok=True)
        self.max_name = cfg["naming"].get("max_filename_chars", 120)

    def folder_path(self, breadcrumb: list[str]) -> list[str]:
        # Drop the literal "Home" root if it's the first crumb; use breadcrumb[1:] otherwise
        parts = [sanitize_path_component(b) for b in breadcrumb if b]
        if parts and parts[0].lower() in ("home", "mcc", "medical counselling committee"):
            parts = parts[1:]
        return parts or ["_root"]

    def filename(self, rec: PdfRecord) -> str:
        # Prefer the visible link label (what the user clicks on the MCC page);
        # fall back to the nearest heading; last resort is the URL's basename.
        title = (rec.link_text or rec.heading or "").strip()
        if title:
            base = sanitize_name(title, self.max_name - 4)  # leave room for ".pdf"
            if not base.lower().endswith(".pdf"):
                base = f"{base}.pdf"
            return base
        original = os.path.basename(urllib.parse.urlparse(rec.url).path) or "file.pdf"
        if not original.lower().endswith(".pdf"):
            original += ".pdf"
        return sanitize_name(original, self.max_name)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def download(self, rec: PdfRecord) -> bool:
        folder = self.staging.joinpath(*self.folder_path(rec.breadcrumb))
        folder.mkdir(parents=True, exist_ok=True)
        fname = self.filename(rec)
        dest = folder / fname
        r = self.session.get(rec.url, stream=True, timeout=60, allow_redirects=True)
        r.raise_for_status()
        h = hashlib.sha256()
        size = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(64 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                h.update(chunk)
                size += len(chunk)
        rec.sha256 = h.hexdigest()
        rec.size_bytes = size
        rec.last_modified = r.headers.get("Last-Modified", "")
        rec.local_path = str(dest)
        if not self._valid_pdf(dest):
            rec.status = "quarantined"
            self.log.warning(f"[invalid pdf] {rec.url}")
            return False
        rec.status = "downloaded"
        return True

    @staticmethod
    def _valid_pdf(path: Path) -> bool:
        try:
            with open(path, "rb") as f:
                head = f.read(5)
            if head != b"%PDF-":
                return False
            # deeper validation — optional, pypdf is slow on some PDFs
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Drive uploader (lazy import, only used in full/delta mode)
# ---------------------------------------------------------------------------


class DriveUploader:
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(self, cfg: dict, logger: logging.Logger):
        from googleapiclient.discovery import build

        self.log = logger
        self.cfg = cfg
        creds = self._load_credentials(cfg)
        self.svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        self.root_id = cfg["drive"]["root_folder_id"]
        self._folder_cache: dict[tuple[str, ...], str] = {(): self.root_id}
        # parent_id -> set of lowercase filenames already taken inside that parent.
        # Populated lazily on first write to each folder; kept up-to-date across the run.
        self._taken_names: dict[str, set[str]] = {}

    def _load_credentials(self, cfg: dict):
        """
        Supports two auth types (config: drive.auth_type):
          - "service_account" (default): robot credentials; requires a Shared Drive for uploads.
            Only works on paid Google Workspace — will fail on personal gmail.com with
            storageQuotaExceeded, because service accounts have no storage quota of their own.
          - "oauth_user": interactive browser consent on first run, then a refresh-token is
            cached locally for silent re-use. Files are owned by the consenting user and
            count against their 15 GB personal quota.
        """
        auth_type = cfg["drive"].get("auth_type", "service_account")
        if auth_type == "service_account":
            from google.oauth2 import service_account
            return service_account.Credentials.from_service_account_file(
                cfg["drive"]["service_account_json"], scopes=self.SCOPES
            )
        if auth_type == "oauth_user":
            from google.oauth2.credentials import Credentials as UserCredentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request

            client_secret_path = cfg["drive"]["oauth_client_secret_json"]
            token_path = cfg["drive"].get("oauth_token_cache", "./credentials/token.json")
            creds = None
            if os.path.exists(token_path):
                try:
                    creds = UserCredentials.from_authorized_user_file(token_path, self.SCOPES)
                except Exception as e:
                    self.log.warning(f"[oauth] cached token unreadable, re-authenticating: {e}")
                    creds = None
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    self.log.warning(f"[oauth] token refresh failed, re-authenticating: {e}")
                    creds = None
            if not creds or not creds.valid:
                self.log.info("[oauth] opening browser for one-time consent — approve in the window that appears")
                flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, self.SCOPES)
                creds = flow.run_local_server(port=0)
                Path(token_path).parent.mkdir(parents=True, exist_ok=True)
                with open(token_path, "w", encoding="utf-8") as f:
                    f.write(creds.to_json())
                self.log.info(f"[oauth] refresh token cached at {token_path}")
            return creds
        raise ValueError(f"Unknown drive.auth_type: {auth_type!r} (expected 'service_account' or 'oauth_user')")

    def _ensure_folder(self, parts: tuple[str, ...]) -> str:
        if parts in self._folder_cache:
            return self._folder_cache[parts]
        parent = self._ensure_folder(parts[:-1])
        name = parts[-1]
        q = (f"mimeType='application/vnd.google-apps.folder' "
             f"and name='{name.replace(chr(39), chr(92)+chr(39))}' "
             f"and '{parent}' in parents and trashed=false")
        resp = self.svc.files().list(q=q, fields="files(id,name)", pageSize=2).execute()
        files = resp.get("files", [])
        if files:
            folder_id = files[0]["id"]
        else:
            folder = self.svc.files().create(
                body={"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]},
                fields="id",
            ).execute()
            folder_id = folder["id"]
            self.log.info(f"[drive] created folder {'/'.join(parts)}")
        self._folder_cache[parts] = folder_id
        return folder_id

    def _load_taken_names(self, parent_id: str) -> set[str]:
        """List every non-trashed child of parent_id and cache their lowercased names."""
        if parent_id in self._taken_names:
            return self._taken_names[parent_id]
        names: set[str] = set()
        page_token = None
        while True:
            resp = self.svc.files().list(
                q=f"'{parent_id}' in parents and trashed=false",
                fields="nextPageToken, files(name)",
                pageSize=1000, pageToken=page_token,
            ).execute()
            for f in resp.get("files", []):
                names.add(f["name"].lower())
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        self._taken_names[parent_id] = names
        return names

    def _allocate_name(self, parent_id: str, desired: str) -> str:
        """Return `desired` if free in parent_id; otherwise suffix with (2), (3), ... until unique.
        Mutates the cache so sibling uploads in the same run see the newly-taken name."""
        taken = self._load_taken_names(parent_id)
        if desired.lower() not in taken:
            taken.add(desired.lower())
            return desired
        stem, ext = os.path.splitext(desired)
        i = 2
        while True:
            candidate = f"{stem} ({i}){ext}"
            if candidate.lower() not in taken:
                taken.add(candidate.lower())
                return candidate
            i += 1

    def upload(self, rec: PdfRecord, folder_parts: list[str], filename: str) -> str:
        from googleapiclient.http import MediaFileUpload

        parent_id = self._ensure_folder(tuple(folder_parts))
        media = MediaFileUpload(rec.local_path, mimetype="application/pdf", resumable=True)
        description = json.dumps({
            "source_url": rec.url,
            "crawl_date": datetime.now(timezone.utc).isoformat(),
            "link_text": rec.link_text,
            "breadcrumb": rec.breadcrumb,
        }, ensure_ascii=False)

        if rec.drive_file_id:
            # update existing as new revision (name unchanged here — it was allocated on first upload)
            updated = self.svc.files().update(
                fileId=rec.drive_file_id, media_body=media,
                body={"name": filename, "description": description},
                fields="id",
            ).execute()
            return updated["id"]

        # New upload — resolve same-folder name collisions so we don't land two "Notice.pdf" files in one folder.
        final_name = self._allocate_name(parent_id, filename)
        created = self.svc.files().create(
            media_body=media,
            body={"name": final_name, "parents": [parent_id], "description": description},
            fields="id",
        ).execute()
        return created["id"]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run(mode: str):
    cfg = load_config()
    logs_dir = Path(cfg["paths"]["logs"]).resolve()
    log = setup_logging(logs_dir)
    log.info(f"=== MCC pipeline: mode={mode} ===")

    manifest = Manifest(Path(cfg["paths"]["manifest"]).resolve())
    run_id = datetime.now(timezone.utc).strftime("run-%Y%m%d-%H%M%S")
    started = datetime.now(timezone.utc).isoformat()

    crawler = Crawler(cfg, log)
    downloader = Downloader(cfg, log)
    drive = DriveUploader(cfg, log) if mode in ("full", "delta") else None

    new = updated = skipped = failed = 0
    records = crawler.crawl()
    total = len(records)
    log.info(f"starting downloads: {total} pdf(s) queued")

    for i, rec in enumerate(records, start=1):
        if i % 25 == 0 or i == 1:
            log.info(f"[progress] {i}/{total}  new={new} updated={updated} skipped={skipped} failed={failed}")
        try:
            existing = manifest.get(rec.url)
            if mode == "delta" and existing and existing["status"] == "uploaded":
                # cheap skip: assume content unchanged unless Last-Modified moved
                head = requests.head(rec.url, allow_redirects=True, timeout=15,
                                     headers={"User-Agent": cfg["crawler"]["user_agent"]})
                if head.headers.get("Last-Modified", "") == (existing["last_modified"] or ""):
                    skipped += 1
                    continue

            ok = downloader.download(rec)
            if not ok:
                failed += 1
                continue

            folder_parts = downloader.folder_path(rec.breadcrumb)
            fname = downloader.filename(rec)

            is_update = bool(existing and existing["sha256"] and existing["sha256"] != rec.sha256)
            is_new = not existing

            if drive:
                rec.drive_file_id = existing["drive_file_id"] if existing and existing.get("drive_file_id") else ""
                rec.drive_file_id = drive.upload(rec, folder_parts, fname)
                rec.status = "uploaded"
                # Free local disk immediately after a successful Drive upload
                if cfg.get("streaming", {}).get("delete_after_upload", True):
                    try:
                        os.unlink(rec.local_path)
                    except OSError:
                        pass
            else:
                rec.status = "downloaded"

            manifest.upsert(rec, drive_path="/".join(folder_parts), drive_filename=fname)
            if is_new:
                new += 1
            elif is_update:
                updated += 1
            else:
                skipped += 1  # re-download but identical
        except Exception as e:
            failed += 1
            log.exception(f"[error] {rec.url}: {e}")

    finished = datetime.now(timezone.utc).isoformat()
    manifest.record_run(run_id, mode, started, finished, new, updated, skipped, failed)
    log.info(f"mode={mode}  new={new}  updated={updated}  skipped={skipped}  failed={failed}")

    # Build a session summary for downstream notifiers. "New files" = pdfs whose
    # first_seen falls inside this run's window. (Updated files don't have a
    # cheap query — their before/after sha256 isn't stored historically.)
    new_files: list[dict] = []
    try:
        cur = manifest.con.execute(
            """SELECT drive_filename, drive_path, drive_file_id, link_text, heading, url
               FROM pdfs WHERE first_seen >= ? ORDER BY first_seen""",
            (started,),
        )
        for row in cur.fetchall():
            fname, dpath, dfid, ltext, head, url = row
            new_files.append({
                "filename": fname,
                "drive_path": dpath,
                "drive_link": (
                    f"https://drive.google.com/file/d/{dfid}/view" if dfid else ""
                ),
                "title": ltext or head or fname,
                "source_url": url,
            })
    except Exception as e:
        log.warning(f"could not enumerate new files for summary: {e}")

    return {
        "pipeline": "main_mcc",
        "run_id": run_id,
        "mode": mode,
        "new": new,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "new_files": new_files,
        "started_at": started,
        "finished_at": finished,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["crawl-only", "full", "delta"], default="crawl-only")
    args = ap.parse_args()
    run(args.mode)


if __name__ == "__main__":
    main()
