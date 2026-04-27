# MCC PDF Pipeline — Step-by-Step Setup Guide

This is the "how to actually build and run it" companion to `MCC_PDF_Pipeline_Plan.md`. Follow the steps in order. Copy-paste-friendly.

---

## Step 0 — What you need installed

On the machine that will run the pipeline:

- **Python 3.11+** — check with `python --version`
- **Git** — optional but recommended
- A folder to hold the project. I'll use `C:\MCC-Pipeline\` in Windows examples and `~/mcc-pipeline/` on Linux/Mac.

---

## Step 1 — Create the project folder and virtual environment

### Windows (PowerShell)
```powershell
mkdir C:\MCC-Pipeline
cd C:\MCC-Pipeline
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### Linux / Mac
```bash
mkdir ~/mcc-pipeline && cd ~/mcc-pipeline
python3 -m venv .venv
source .venv/bin/activate
```

---

## Step 2 — Install the dependencies

Create a file called `requirements.txt` with this content:

```
requests==2.32.3
beautifulsoup4==4.12.3
lxml==5.3.0
playwright==1.48.0
pypdf==5.1.0
google-api-python-client==2.149.0
google-auth==2.35.0
google-auth-httplib2==0.2.0
pyyaml==6.0.2
tenacity==9.0.0
```

Then install:

```bash
pip install -r requirements.txt
playwright install chromium
```

The second line downloads the headless Chromium browser that Playwright uses as a fallback when a page renders its menu via JavaScript.

---

## Step 3 — Set up Google Cloud service account

You chose the service-account auth route. Here's the exact clickpath:

1. Go to https://console.cloud.google.com/ and sign in as **teamtabindia@gmail.com**.
2. **Create a new project.** Top-left project dropdown → *New Project* → name it `mcc-pdf-mirror` → Create.
3. **Enable the Drive API.** In the left nav, *APIs & Services* → *Library* → search "Google Drive API" → Enable.
4. **Create the service account.** *APIs & Services* → *Credentials* → *Create Credentials* → *Service Account*.
   - Name: `mcc-pipeline-bot`
   - Role: leave blank (Drive permissions come via folder sharing, not IAM) — skip the "Grant access" step.
   - Click *Done*.
5. **Generate a JSON key.** Click the service account you just made → *Keys* tab → *Add Key* → *Create new key* → JSON → Create. A file downloads, something like `mcc-pdf-mirror-abc123.json`.
6. **Move and rename that file** to `C:\MCC-Pipeline\credentials\service-account.json` (create the `credentials` folder).
7. **Note the service account email.** It looks like `mcc-pipeline-bot@mcc-pdf-mirror.iam.gserviceaccount.com`. You'll need it in the next step.

### Share a Drive folder with the service account

8. Open Google Drive as **teamtabindia@gmail.com**.
9. Create a new folder named **`MCC Archive`** at the root of *My Drive*.
10. Right-click it → *Share* → paste the service account email → set to **Editor** → *Send*.
11. Open the folder. The URL looks like `https://drive.google.com/drive/folders/1aBcDeFgHiJkLmNoPqRsTuVwXyZ`. Copy the ID after `/folders/` — that's the **Drive root folder ID**. Save it for the config step.

---

## Step 4 — Project layout

Inside `C:\MCC-Pipeline\` create this layout:

```
C:\MCC-Pipeline\
├── .venv\                       (already made)
├── credentials\
│   └── service-account.json     (already placed)
├── src\
│   ├── crawler.py
│   ├── resolver.py
│   ├── tagger.py
│   ├── downloader.py
│   ├── manifest.py
│   ├── drive.py
│   └── main.py
├── logs\                        (auto-created on first run)
├── staging\                     (auto-created on first run)
├── config.yaml
└── requirements.txt             (already made)
```

The starter script I'm shipping with this guide (`mcc_pipeline_starter.py`) is a single-file version you can run immediately — the layout above is the cleaned-up multi-module version you migrate to once you've confirmed it works.

---

## Step 5 — Create the config file

Save this as `C:\MCC-Pipeline\config.yaml`. **Replace the two placeholders.**

```yaml
seed_url: "https://mcc.nic.in/"
domain: "mcc.nic.in"

paths:
  staging: "./staging"
  manifest: "./manifest.db"
  logs: "./logs"

crawler:
  polite_delay_seconds: 1.0
  max_pages: 2000
  user_agent: "MCC-PDF-Mirror/1.0 teamtabindia@gmail.com"
  respect_robots_txt: true

drive:
  service_account_json: "./credentials/service-account.json"
  root_folder_id: "REPLACE_WITH_MCC_ARCHIVE_FOLDER_ID"
  owner_email: "teamtabindia@gmail.com"

naming:
  format: "heading_plus_filename"   # matches what you picked
  max_filename_chars: 120
```

---

## Step 6 — First run: crawl + download only (no Drive yet)

Before wiring Drive in, prove the crawler and downloader work end-to-end against MCC.

```bash
python mcc_pipeline_starter.py --mode crawl-only
```

This walks the site, resolves every PDF link, downloads to `./staging/`, and populates `manifest.db`. Expect 5–20 minutes on first run depending on your connection and MCC's responsiveness.

When it finishes, check:
- `./staging/` should contain PDFs organized in subfolders matching the breadcrumb.
- `manifest.db` can be inspected with any SQLite viewer (DB Browser for SQLite is free).
- `./logs/run-<timestamp>.log` should end with a summary line like `new=143 updated=0 skipped=0 failed=2`.

---

## Step 7 — Second run: full pipeline with Drive upload

Once stage 6 looks clean:

```bash
python mcc_pipeline_starter.py --mode full
```

This repeats the crawl (everything is already cached, so it's fast), then uploads every file in `manifest.db` whose `drive_file_id` is NULL. Folder tree in `MCC Archive/` is auto-created.

Open Drive, navigate into `MCC Archive/`, confirm the folder structure matches the site navigation.

---

## Step 8 — Schedule it

You asked for initial bulk + scheduled delta. Once step 7 is successful, register a recurring job.

### Windows — Task Scheduler

Create a task:
- **Trigger:** Daily, 06:00 IST
- **Action:** Start a program
  - Program: `C:\MCC-Pipeline\.venv\Scripts\python.exe`
  - Arguments: `C:\MCC-Pipeline\mcc_pipeline_starter.py --mode delta`
  - Start in: `C:\MCC-Pipeline\`

### Linux — cron

```bash
crontab -e
```

Add:
```
0 6 * * *  cd /home/you/mcc-pipeline && /home/you/mcc-pipeline/.venv/bin/python mcc_pipeline_starter.py --mode delta >> logs/cron.log 2>&1
```

Delta mode skips every PDF whose URL is already in the manifest with an unchanged SHA-256, so daily runs take a minute or two and only upload genuinely new/changed files.

---

## Step 9 — Verify delta behavior

Right after a delta run, open the latest file in `./logs/`. The summary line tells you what happened:

```
mode=delta  new=3  updated=1  skipped=1402  failed=0  elapsed=87s
```

- `new` = URLs the crawler saw for the first time
- `updated` = known URLs whose SHA-256 changed (new revision uploaded to Drive)
- `skipped` = known URLs, unchanged content (no Drive call made)
- `failed` = logged per-URL in the same file with the error

If `skipped` stays near the total PDF count across runs, delta mode is working correctly.

---

## Common problems

| Symptom | Cause | Fix |
|---|---|---|
| `403 Forbidden` from Drive API | Root folder not shared with service account | Re-share `MCC Archive/` with the service account email as Editor |
| `Playwright executable not found` | Chromium not installed | Run `playwright install chromium` |
| Crawler finds only the homepage links | JS-rendered menu | Confirm Playwright fallback fires — look for `[playwright] rendering …` lines in the log |
| Same PDF uploaded twice under different names | URL aliasing — two URLs, same content | Already handled: manifest dedupes by SHA-256, second URL is logged as an alias |
| MCC returning 503 | Rate limit | Raise `polite_delay_seconds` in `config.yaml` to `2.0` |

---

## What to do next

Once the scheduled job has run clean for a few days:
- Add the **email summary** notifier (stage 8 of the plan) so you get a daily one-line report.
- Move from the single-file starter to the multi-module layout in section 4 of the plan — easier to test and maintain.
- Consider a **monthly snapshot** of `manifest.db` uploaded to `MCC Archive/_manifest/` so you have a versioned index of the whole archive over time.
