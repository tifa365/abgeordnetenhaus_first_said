"""Re-OCR Batch: Verarbeitet eine Liste von Seiten-IDs.

Aufruf:
    uv run python utilities/reocr_batch.py '[[page_id, doc_id, page_no, "doknr"], ...]'
"""

import sys
import os
import json
import logging
import subprocess
import fcntl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pymupdf
from database import _get_connection

ARCHIVE_DIR = os.path.join(os.path.dirname(__file__), '..', 'archive')
PROGRESS_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'reocr_progress.json')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def ocr_page(pdf_path, page_no_1based):
    """Extrahiert Text einer PDF-Seite mit Tesseract OCR."""
    doc = pymupdf.open(pdf_path)
    page = doc[page_no_1based - 1]
    pix = page.get_pixmap(dpi=150)
    pid = os.getpid()
    img_path = f'/tmp/reocr_page_{pid}.png'
    pix.save(img_path)
    doc.close()

    out_base = f'/tmp/reocr_out_{pid}'
    out_path = out_base + '.txt'

    try:
        result = subprocess.run(
            ['tesseract', img_path, out_base, '-l', 'deu'],
            capture_output=True, text=True, timeout=600
        )
        if result.returncode != 0:
            logging.warning(f"Tesseract Fehler: {result.stderr[:200]}")
            return None
        with open(out_path, encoding='utf-8') as f:
            return f.read()
    except subprocess.TimeoutExpired:
        logging.warning(f"Tesseract Timeout fuer {pdf_path} Seite {page_no_1based}")
        return None
    finally:
        for path in (img_path, out_path):
            if os.path.exists(path):
                os.remove(path)


def append_progress(page_id):
    """Haengt eine page_id an die Progress-Datei an (mit File-Lock)."""
    os.makedirs(os.path.dirname(PROGRESS_PATH), exist_ok=True)
    with open(PROGRESS_PATH, 'r+', encoding='utf-8') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        done = json.load(f)
        done.append(page_id)
        f.seek(0)
        f.truncate()
        json.dump(sorted(done), f)
        fcntl.flock(f, fcntl.LOCK_UN)


def main():
    pages = json.loads(sys.argv[1])
    logging.info(f"Batch: {len(pages)} Seiten zu verarbeiten")

    conn = _get_connection()
    ok = 0
    fail = 0

    for page_id, doc_id, page_no, doknr in pages:
        safe_doknr = doknr.replace('/', '-')
        pdf_path = os.path.join(ARCHIVE_DIR, f'{safe_doknr}.pdf')

        if not os.path.exists(pdf_path):
            logging.warning(f"PDF nicht gefunden: {pdf_path}")
            append_progress(page_id)
            fail += 1
            continue

        logging.info(f"OCR: {doknr} Seite {page_no}")
        ocr_text = ocr_page(pdf_path, page_no)

        if ocr_text is None:
            logging.warning(f"  OCR fehlgeschlagen")
            append_progress(page_id)
            fail += 1
            continue

        conn.execute(
            "UPDATE pages SET text = ?, char_count = ? WHERE id = ?",
            (ocr_text, len(ocr_text), page_id)
        )
        conn.commit()
        append_progress(page_id)
        ok += 1

    logging.info(f"Batch fertig: {ok} OK, {fail} fehlgeschlagen")


if __name__ == "__main__":
    main()
