# Daily MCC mirror on GitHub Actions — setup guide

This file walks through getting the three pipelines (main MCC + admissions PG + admissions UG)
running in the cloud once a day, for free, with an email acknowledgement.

You'll do this **once**. After that everything is automatic.

---

## What you need before starting

- A **GitHub account** (free — sign up at https://github.com if you don't have one).
- The contents of two local files: `credentials/token.json` and `credentials/client_secret.json`.
- Your Gmail App Password (the 16-char one already in `config.yaml`).

You'll do everything below in your browser. No need to install anything new.

---

## Step 1 — Create a private GitHub repo and push the code

1. Go to https://github.com/new
2. Repository name: `mcc-mirror` (or anything you like).
3. Visibility: **Private** is fine. **Public** is also fine and gives you unlimited Actions minutes; secrets stay private regardless.
4. Don't initialize with README/license/.gitignore — we already have those.
5. Click **Create repository**. GitHub will show a "...or push an existing repository" snippet — keep that page open.

In PowerShell, from the `PDF Extractor` folder:

```powershell
cd "C:\Users\Aman Kumar\OneDrive\Desktop\PDF Extractor"
git init
git add .
git commit -m "Initial commit — MCC mirror pipelines + GitHub Actions"
git branch -M main
git remote add origin https://github.com/<YOUR-USERNAME>/mcc-mirror.git
git push -u origin main
```

`.gitignore` already excludes `credentials/`, `manifest.db`, `staging/`, `.venv/`, `logs/`, etc., so none of your secrets or local cruft get pushed.

---

## Step 2 — Make the OAuth refresh token long-lived (one-time, important)

By default, OAuth apps in **Testing** status have refresh tokens that expire after **7 days**. That would break daily scheduling. To fix:

1. Go to https://console.cloud.google.com/auth/audience
2. Select the same project you used to create the Drive OAuth app.
3. If "Publishing status" says **Testing**, click **Publish App** → confirm.
4. You'll see a "Verification status: Unverified" banner — that's fine for personal use. Just acknowledge any warnings.

Now the refresh token in `credentials/token.json` lasts indefinitely (until revoked).

> If you skip this step, the GitHub Actions runs will start failing after about a week with `invalid_grant`. You'd then need to re-run the pipeline locally, which auto-refreshes the token, and re-paste it as a secret. Annoying but recoverable.

---

## Step 3 — Add four secrets to your GitHub repo

In your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**.

Add these four, exactly as named. Values shown in `<>` brackets — paste raw values, no quotes.

| Secret name                          | Value                                                                                  |
|--------------------------------------|----------------------------------------------------------------------------------------|
| `GOOGLE_OAUTH_TOKEN_JSON`            | The **entire contents** of your local `credentials/token.json` file (open in Notepad → Ctrl-A → Ctrl-C → paste). |
| `GOOGLE_OAUTH_CLIENT_SECRET_JSON`    | The **entire contents** of `credentials/client_secret.json`.                            |
| `GMAIL_APP_PASSWORD`                 | The 16-character Gmail App Password (without spaces, e.g. `abcdwxyz1234abcd`).         |
| `NOTIFY_EMAIL_TO`                    | `teamtabindia@gmail.com` (or any other recipient).                                      |

---

## Step 4 — Run it manually once to verify

1. Go to your repo's **Actions** tab.
2. On the left, click **MCC Daily Mirror**.
3. Click **Run workflow** (top right) → leave defaults → **Run workflow**.
4. A new run appears under "All workflows". Click it to watch live logs.

The first manual run will take 30-90 minutes:
- Main pipeline: ~5-15 min in delta mode (mostly already mirrored)
- PG admissions: ~30-60 min for 616 institutes
- UG admissions: ~60-90 min for thousands of institutes
- Email goes out at the end

If anything fails:
- Click the failing step to see the error
- Most commonly: missing/wrong secret, or the `invalid_grant` from skipping Step 2

---

## Step 5 — That's it. The cron does the rest.

The workflow now fires at **01:30 UTC = 07:00 IST every day**. Your email arrives shortly after, listing whatever's new across all three sources.

To **change the schedule**, edit `.github/workflows/mcc-daily.yml`:
```yaml
on:
  schedule:
    - cron: "30 1 * * *"     # change this
```
(Cron format: `minute hour day-of-month month day-of-week`. Use https://crontab.guru to read/write expressions.)

To **stop the schedule** temporarily: Actions tab → MCC Daily Mirror → "..." → Disable workflow.

---

## How state survives between runs

GitHub Actions runners get a fresh disk on every run. Two things therefore live on Drive in a hidden `_state/` folder under `MCC Archive`:
- `manifest.db` — the SQLite manifest of every PDF ever seen by the main pipeline
- `PG_last_state.json` and `UG_last_state.json` — institute-code snapshots for delta detection

`run_all.py` automatically pulls these at the start of every run and pushes the updated copies at the end. You don't need to do anything.

---

## Troubleshooting cheat sheet

| Symptom                                 | Fix                                                                                  |
|-----------------------------------------|--------------------------------------------------------------------------------------|
| Run fails with `invalid_grant`          | Step 2 wasn't done, or the refresh token was revoked. Re-run locally to get a new `token.json`, paste into `GOOGLE_OAUTH_TOKEN_JSON` secret. |
| Email never arrives                     | Open the run logs, search for `notify:`. Either `disabled in config`, `no changes` (your `skip_if_no_changes: true`), or SMTP error. |
| Same files re-uploaded every day        | State pull failed. Check logs for `[state] manifest.db: pull failed`. Likely a Drive perms issue on the `_state` folder. |
| Workflow doesn't appear in Actions tab  | Did you push the `.github/workflows/` folder? `git status` should show no untracked files; if it does, `git add .` then push again. |
| Run takes too long, hits 4-hour timeout | Edit `timeout-minutes` in the workflow file. Or reduce frequency to weekly.          |
| Free minutes running out (private repo) | Switch the repo to Public — secrets stay private, Actions minutes become unlimited.  |
