"""
One-shot patch: adds the section-heading-as-subfolder logic to
mcc_pipeline_starter.py. Safe to re-run (idempotent).

Run from PowerShell:
    cd "C:\Users\Aman Kumar\OneDrive\Desktop\PDF Extractor"
    python apply_heading_patch.py
"""
from pathlib import Path

PATH = Path("mcc_pipeline_starter.py")

OLD = """                        bc = breadcrumb
                        meaningful = [c for c in bc if c.lower() not in
                                      ("home", "mcc", "medical counselling committee")]
                        if not meaningful:
                            url_bc = self._breadcrumb_from_url(final_url)
                            if url_bc:
                                bc = bc + url_bc
                        pdf_records[resolved] = PdfRecord("""

NEW = """                        bc = list(breadcrumb)
                        meaningful = [c for c in bc if c.lower() not in
                                      ("home", "mcc", "medical counselling committee")]
                        if not meaningful:
                            url_bc = self._breadcrumb_from_url(final_url)
                            if url_bc:
                                bc = bc + url_bc
                        # Add the in-page section heading as a deeper folder.
                        # Catches sub-categories like "News & Events", "Schedule",
                        # "Candidate Activity Board", "Important Links" that are
                        # h2/h3 sections WITHIN a page rather than separate pages.
                        if heading:
                            section = heading.strip()[:80]
                            if (section
                                and (not bc or section.lower() != bc[-1].lower())
                                and section.lower() not in (
                                    "home", "mcc", "medical counselling committee",
                                    "menu", "main menu", "navigation",
                                    "skip to main content",
                                )):
                                bc = bc + [section]
                        bc = bc[:MAX_BREADCRUMB_DEPTH]
                        pdf_records[resolved] = PdfRecord("""


def main():
    text = PATH.read_text(encoding="utf-8")
    if "Add the in-page section heading as a deeper folder" in text:
        print("[skip] patch already applied")
        return
    if OLD not in text:
        print("[ERROR] anchor block not found - file may be a different version.")
        print("        Try: git checkout HEAD -- mcc_pipeline_starter.py")
        print("        Then re-run this script.")
        return
    text = text.replace(OLD, NEW, 1)
    PATH.write_text(text, encoding="utf-8")
    print(f"[ok] patched {PATH} (+{NEW.count(chr(10)) - OLD.count(chr(10))} lines)")
    # Quick syntax check
    import py_compile
    try:
        py_compile.compile(str(PATH), doraise=True)
        print("[ok] file still compiles")
    except py_compile.PyCompileError as e:
        print(f"[ERROR] file no longer compiles: {e}")


if __name__ == "__main__":
    main()
