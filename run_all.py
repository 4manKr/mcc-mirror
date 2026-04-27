"""
Orchestrator — runs ALL three MCC pipelines in sequence and sends ONE
acknowledgement email at the end.

Pipelines:
    1. Main MCC website (mcc.nic.in)             [delta mode by default]
    2. Admissions Institute Profiles — PG
    3. Admissions Institute Profiles — UG

Usage:
    python run_all.py                       # full daily run + email
    python run_all.py --skip-main           # only run admissions
    python run_all.py --skip-admissions     # only run main
    python run_all.py --limit 2             # smoke test (2 institutes per course)
    python run_all.py --no-email            # run pipelines but don't send email
    python run_all.py --main-mode full      # force full crawl (default: delta)

Reads its email/SMTP settings from config.yaml under the `notify` section.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from mcc_pipeline_starter import (
    run as run_main_pipeline,
    load_config,
    setup_logging,
    DriveUploader,
)
from mcc_admissions_pipeline import (
    LISTINGS as ADMISSIONS_LISTINGS,
    run as run_admissions_pipeline,
)
from notifier import send_run_notification, total_changes
from state_sync import pull_state, push_state


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-main", action="store_true",
                    help="Skip the main mcc.nic.in pipeline")
    ap.add_argument("--skip-admissions", action="store_true",
                    help="Skip the admissions (PG + UG) pipelines")
    ap.add_argument("--main-mode", choices=["crawl-only", "full", "delta"],
                    default="delta",
                    help="Mode for the main pipeline (default: delta)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Limit institutes per course (smoke test only)")
    ap.add_argument("--headed", action="store_true",
                    help="Run admissions Chromium with visible window")
    ap.add_argument("--no-email", action="store_true",
                    help="Run pipelines but don't send the summary email")
    args = ap.parse_args()

    cfg = load_config()
    logs_dir = Path(cfg["paths"]["logs"]).resolve()
    log = setup_logging(logs_dir)

    log.info("\n" + "#"*64)
    log.info("##  MCC ORCHESTRATOR — running all configured pipelines  ##")
    log.info("#"*64)

    # Single DriveUploader shared across all pipelines (one OAuth load, less overhead)
    drive = DriveUploader(cfg, log)
    summaries: list[dict] = []

    # Pull persistent state (manifest + per-course snapshots) from Drive.
    # Critical on ephemeral runners (GitHub Actions); harmless locally.
    try:
        pull_state(cfg, drive, log)
    except Exception as e:
        log.exception(f"state pull failed (continuing anyway): {e}")

    # ------------------------------------------------------------ Main MCC
    if not args.skip_main:
        log.info("\n" + "="*64 + "\n  PIPELINE 1/3 — Main MCC website\n" + "="*64)
        try:
            s = run_main_pipeline(args.main_mode, limit=args.limit)
            if s:
                summaries.append(s)
        except Exception as e:
            log.exception(f"main pipeline crashed: {e}")
    else:
        log.info("(skipping main pipeline per --skip-main)")

    # ------------------------------------------------------------ Admissions
    if not args.skip_admissions:
        for idx, (course, url) in enumerate(ADMISSIONS_LISTINGS, start=2):
            log.info("\n" + "="*64 +
                     f"\n  PIPELINE {idx}/3 — Admissions ({course})\n" + "="*64)
            try:
                s = run_admissions_pipeline(
                    listing_url=url,
                    limit=args.limit,
                    headless=not args.headed,
                    course=course,
                    drive=drive,
                    log=log,
                )
                if s:
                    summaries.append(s)
            except Exception as e:
                log.exception(f"admissions [{course}] crashed: {e}")
    else:
        log.info("(skipping admissions pipelines per --skip-admissions)")

    # ------------------------------------------------------------ Push state
    try:
        push_state(cfg, drive, log)
    except Exception as e:
        log.exception(f"state push failed (run still considered successful): {e}")

    # ------------------------------------------------------------ Summary + Email
    log.info("\n" + "#"*64)
    log.info("##  ALL PIPELINES FINISHED  ##")
    log.info("#"*64)
    for s in summaries:
        kind = s.get("pipeline")
        if kind == "main_mcc":
            log.info(f"  [main]    new={s.get('new')} updated={s.get('updated')} "
                     f"skipped={s.get('skipped')} failed={s.get('failed')}")
        elif kind and kind.startswith("admissions"):
            log.info(f"  [{s.get('course')}]      institutes={s.get('institutes')} "
                     f"profile={s.get('profile_uploaded')} "
                     f"bond={s.get('bond_uploaded')} "
                     f"new_inst={len(s.get('new_institutes', []))}")
    log.info(f"  TOTAL CHANGES: {total_changes(summaries)}")

    if args.no_email:
        log.info("(--no-email set — skipping email)")
        return

    log.info("\nSending acknowledgement email ...")
    sent = send_run_notification(cfg, summaries, log)
    if sent:
        log.info("Email acknowledgement delivered.")
    else:
        log.info("No email sent (either disabled, no changes, or send failed — see logs).")


if __name__ == "__main__":
    main()
