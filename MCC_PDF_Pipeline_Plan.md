# MCC.nic.in PDF Extraction Pipeline — System Design

**Target site:** https://mcc.nic.in/ (Medical Counselling Committee, India)
**Goal:** Crawl the entire site, download every PDF (including ones reached through redirects and nested sub-pages), and mirror them into Google Drive in a folder hierarchy that matches the site's navigation, with filenames that carry the page heading as context.
**Account:** teamtabindia@gmail.com
**Run model:** Initial bulk crawl, then scheduled delta runs that only grab new or changed PDFs.

---

## 1. High-level architecture

```
  ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
  │  Scheduler      │ ──► │  Crawler         │ ──► │  Link resolver   │
  │  (cron/systemd) │     │  (requests + BS4 │     │  (follow 301/302,│
  │                 │     │   + Playwright)  │     │   sniff Content- │
  └─────────────────┘     └──────────────────┘     │   Type)          │
                                   │                └──────────────────┘
                                   ▼                         │
                          ┌──────────────────┐               ▼
                          │  Breadcrumb /    │     ┌──────────────────┐
                          │  heading tagger  │ ──► │  Downloader      │
                          └──────────────────┘     │  (retry+backoff) │
                                                   └──────────────────┘
                                                            │
                                                            ▼
                     ┌───────────────────┐         ┌──────────────────┐
                     │ SQLite manifest   │ ◄─────► │  Drive uploader  │
                     │ (url, sha256,     │         │  (service acct)  │
                     │  drive_id, path)  │         └──────────────────┘
                     └───────────────────┘                  │
                                                            ▼
                                              ┌─────────────────────────┐
                                              │  Google Drive           │
                                              │  MCC/<Section>/<Sub>/…  │
                                              └─────────────────────────┘
```

Seven loosely coupled stages. Each can be swapped out, retried, or scaled independently.

---

## 2. Stage-by-stage design

### 2.1 Scheduler
A wrapper that triggers the pipeline. Two modes:

- **Bulk mode** (`--full`): ignore the manifest, re-fetch and re-verify everything. Used on first run and occasionally for integrity audits.
- **Delta mode** (`--delta`): default scheduled run. Only downloads PDFs whose URL is new, whose `Last-Modified` header changed, or whose SHA-256 differs from the manifest.

Implementation: `cron` on Linux or Windows Task Scheduler on the user's machine. A simple daily 06:00 IST job is plenty for MCC — announcements don't come out hourly.

### 2.2 Crawler
Seeds from `https://mcc.nic.in/` and walks the site breadth-first.

- HTTP client: `requests.Session` with a custom User-Agent (`MCC-PDF-Mirror/1.0 teamtabindia@gmail.com`) and 1s polite delay.
- HTML parser: `BeautifulSoup` (lxml backend).
- JS-rendered pages: `Playwright` (headless Chromium) fallback when the static HTML yields zero links — MCC has some ASPX menus that render client-side.
- Stays on the `mcc.nic.in` domain. External hosts are recorded but not crawled (per your scope choice).
- Honors `robots.txt`.
- Dedupes URLs by normalized form (lowercased host, stripped fragments, sorted query params).
- Maintains the **breadcrumb trail** for every URL: the chain of page titles from the homepage to this page. This is what drives the Drive folder structure later.

### 2.3 Link resolver
For every outbound link discovered on a page, decide: is this a PDF?

Three cases to handle:
1. **Direct `.pdf` link** — trivial, queue it for download.
2. **Redirects to PDF** — the link is `/showpdf.aspx?id=1234` or similar; issue a `HEAD` request, follow redirects (`allow_redirects=True`), and inspect the final `Content-Type`. If it's `application/pdf`, queue it.
3. **Links with no extension but PDF payload** — some government pages serve PDFs with generic URLs. Fall back to `GET` with a `Range: bytes=0-4` header and check for the `%PDF-` magic bytes.

Each resolved PDF is stored with: final URL, original link text (the `<a>` innerText — usually the "headline"), the nearest preceding heading (`h1`/`h2`/`h3`) on the page it was found, and the breadcrumb path.

### 2.4 Breadcrumb / heading tagger
This is what turns raw downloads into an organized archive.

For every PDF, record:
- **Breadcrumb path** → drives Drive folder structure, e.g. `Home > UG Medical > Counselling Schedule`.
- **Link text** → usually the "headline" (e.g. "Revised Schedule for Round 2 – 2026").
- **Nearest heading** → the `<h1>/<h2>/<h3>` on the linking page above the anchor. Used as the "section" if the breadcrumb is shallow.

Folder path rule: `MCC/<Top-level nav item>/<Sub-section>/<Sub-sub-section>/`
Filename rule: `<nearest heading> — <original filename>.pdf` (sanitized, 120-char cap).

Example:

| Field | Value |
|---|---|
| Source URL | `https://mcc.nic.in/UGMedical/SchedulePDF/UG_R2_Schedule.pdf` |
| Breadcrumb | Home → UG Medical Counselling → Counselling Schedule |
| Nearest heading | "Round 2 Schedule — 2026" |
| Drive folder | `MCC/UG Medical Counselling/Counselling Schedule/` |
| Drive filename | `Round 2 Schedule — 2026 — UG_R2_Schedule.pdf` |
| Description (Drive metadata) | Source URL + crawl date + link text |

### 2.5 Downloader
- Streams each PDF to a local staging dir (`./staging/<sha_prefix>/<filename>`).
- Retry policy: 3 attempts, exponential backoff (2s, 8s, 30s).
- Validates the file is a real PDF (`PyPDF2.PdfReader(f).pages` smoke test) before marking success.
- Computes SHA-256 for dedup + change detection.

### 2.6 Manifest (SQLite)
Single file `manifest.db`. Schema:

```sql
CREATE TABLE pdfs (
  url            TEXT PRIMARY KEY,
  sha256         TEXT NOT NULL,
  size_bytes     INTEGER,
  last_modified  TEXT,         -- from HTTP header
  drive_file_id  TEXT,         -- set after upload
  drive_path     TEXT,         -- folder path in Drive
  drive_filename TEXT,
  breadcrumb     TEXT,         -- JSON array
  heading        TEXT,
  link_text      TEXT,
  first_seen     TEXT,
  last_checked   TEXT,
  status         TEXT          -- ok | failed | quarantined
);
CREATE TABLE runs (
  run_id     TEXT PRIMARY KEY,
  started_at TEXT,
  finished_at TEXT,
  mode       TEXT,             -- full | delta
  new_count  INTEGER,
  updated_count INTEGER,
  failed_count  INTEGER
);
```

Delta logic: a PDF is "new" if the URL isn't in the manifest. It's "updated" if the URL is known but the SHA-256 differs. Otherwise it's skipped — no re-upload.

### 2.7 Drive uploader
Using a **Google Cloud service account** (your chosen auth option):

**One-time setup:**
1. Create a GCP project (free tier is fine).
2. Enable the Drive API.
3. Create a service account, generate a JSON key.
4. Create a root Drive folder called `MCC Archive` in `teamtabindia@gmail.com`.
5. Share that folder with the service account's email (`…@<project>.iam.gserviceaccount.com`) as **Editor**.
6. Put the folder ID in the pipeline config.

**Runtime behavior:**
- Uses `google-api-python-client` with the service account JSON.
- For each PDF, ensures the folder path exists in Drive (creates folders lazily, caches folder IDs in a second SQLite table).
- Uploads with the **resumable** endpoint (important for large PDFs on flaky connections).
- Sets Drive file metadata: `description` = source URL + crawl date + link text, so you can search Drive later by original context.
- On "updated" PDFs: uploads as a **new revision** of the existing `drive_file_id`, preserving the Drive link and version history.

### 2.8 Observability
- Log file `logs/run-<timestamp>.log` with per-URL status.
- End-of-run summary written to `logs/summary-<timestamp>.md`: counts of new/updated/skipped/failed, top 10 failed URLs with reasons.
- Optional: email the summary to teamtabindia@gmail.com via SMTP.

---

## 3. Folder layout in Google Drive

```
MCC Archive/
├── _manifest/
│   └── manifest-2026-04-24.db         (weekly snapshot of the SQLite file)
├── UG Medical Counselling/
│   ├── Counselling Schedule/
│   │   ├── Round 1 Schedule — UG_R1_Schedule.pdf
│   │   └── Round 2 Schedule — UG_R2_Schedule.pdf
│   ├── Notices/
│   │   └── Public Notice dated 18-Apr-2026 — notice_18apr.pdf
│   ├── Result/
│   │   └── Round 1 Result — UG_R1_Result.pdf
│   └── Seat Matrix/
│       └── AIQ Seat Matrix 2026 — seat_matrix_2026.pdf
├── PG Medical Counselling/
│   └── … (same sub-structure)
├── Services/
│   ├── Schedule UG/
│   ├── Schedule PG/
│   └── Registration Help/
├── Information Bulletin/
└── Contact/
```

Top-level folders mirror the main nav of mcc.nic.in. Sub-folders are auto-created from the breadcrumb trail — no manual mapping table needed.

---

## 4. Project layout (code)

```
mcc-pdf-pipeline/
├── pyproject.toml
├── config.yaml                 # seed URL, Drive folder ID, paths, schedule
├── credentials/
│   └── service-account.json    # .gitignored
├── src/
│   ├── crawler.py              # BFS, Playwright fallback
│   ├── resolver.py             # redirect + content-type sniffing
│   ├── tagger.py               # breadcrumb + heading extraction
│   ├── downloader.py           # retry, validate, hash
│   ├── manifest.py             # SQLite helpers
│   ├── drive.py                # folder-tree + resumable upload
│   ├── scheduler.py            # entrypoint (--full / --delta)
│   └── notify.py               # email/summary
├── tests/
│   └── fixtures/               # saved HTML + sample PDFs
├── logs/
└── staging/
```

Runtime: Python 3.11, ~8 dependencies (`requests`, `beautifulsoup4`, `lxml`, `playwright`, `pypdf`, `google-api-python-client`, `google-auth`, `pyyaml`).

---

## 5. Delivery plan (what gets built, in order)

1. **Day 1 — Crawler + resolver.** Walk mcc.nic.in, print every discovered PDF URL with its breadcrumb and heading. No downloads yet. Verify coverage by spot-checking nav sections manually.
2. **Day 2 — Downloader + manifest.** Pull everything to local disk, populate SQLite. End of day: a full local mirror and a query-able index.
3. **Day 3 — Drive integration.** Service account setup, folder-tree creation, resumable upload, revision handling. End of day: mirror visible in Drive.
4. **Day 4 — Delta mode + scheduler.** Wire up change detection, register the cron/Task Scheduler job, hook up email summary.
5. **Day 5 — Hardening.** Timeouts, Playwright fallback for JS menus, bad-PDF quarantine, retry dead-letter queue, test run end-to-end.

---

## 6. Risks and how the design handles them

- **JavaScript-rendered navigation.** Fallback to Playwright when a page yields zero links in static HTML.
- **Redirects to non-PDF pages.** Content-Type sniffing + magic-byte check before committing a URL as "PDF".
- **Huge PDFs / flaky network.** Resumable Drive uploads + exponential-backoff download retries.
- **MCC rate limiting.** 1s polite delay, single-threaded crawl, honest User-Agent.
- **Duplicate PDFs under different URLs.** SHA-256 dedup — same hash gets a single Drive entry, with a second URL recorded as an alias in the manifest.
- **File-name collisions inside the same folder.** Append a short hash suffix: `… — UG_R1.pdf` → `… — UG_R1 (a7f3).pdf`.
- **Site structure changes.** Breadcrumb is computed per-run, so a section rename just creates a new folder next time; old files stay where they were (with their `drive_file_id` preserved).
- **Service-account quota.** Drive API free tier = 20k requests/100s. We're at most a few hundred ops per run. Comfortable.

---

## 7. What I need from you to start building

1. Confirm the GCP project / service account can be created under teamtabindia@gmail.com (or tell me if you want me to walk through the setup step-by-step).
2. Confirm this will run on your local machine, a VPS, or a cloud scheduler (Cloud Run / GitHub Actions). That changes where cron lives.
3. Confirm the scheduled cadence — daily, twice-weekly, weekly?

Once those three are settled I can start writing stage 1 (the crawler) and give you a list of every PDF on mcc.nic.in before we touch Drive.
