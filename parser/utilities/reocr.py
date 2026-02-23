"""Re-OCR: Erkennt garbled PDF-Seiten und re-extrahiert sie mit Tesseract.

Bestimmte WP12-PDFs haben Font-Encoding-Probleme, bei denen PyMuPDF
falsche Zeichen liest. Tesseract liest die Pixel statt der Font-Tabelle
und liefert korrekten Text.

Nach dem Re-OCR werden betroffene Dokumente automatisch neu verarbeitet
(Woerter neu extrahiert).

Aufruf:
    cd parser && uv run python utilities/reocr.py --dry-run
    cd parser && uv run python utilities/reocr.py --apply
"""

import sys
import os
import json
import logging
import subprocess
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pymupdf
from database import _get_connection, flush

ARCHIVE_DIR = os.path.join(os.path.dirname(__file__), '..', 'archive')
PROGRESS_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'reocr_progress.json')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# Muster die auf Font-Encoding-Garble hinweisen
GARBLE_PATTERNS = [
    '% dte %',       # 'die' garbled
    '% dtem %',      # 'diem' garbled
    '% dtese%',      # 'diese' garbled
    '%Berlt%',       # 'Berlin' garbled
    '%Regterung%',   # 'Regierung' garbled
    '%setner%',      # 'seiner' garbled
    '%ketne%',       # 'keine' garbled
    '%allerdtngs%',  # 'allerdings' garbled
    '%Sttuatton%',   # 'Situation' garbled
]


def find_garbled_pages(conn):
    """Findet Seiten mit Font-Encoding-Garble."""
    conditions = " OR ".join(f"p.text LIKE '{pat}'" for pat in GARBLE_PATTERNS)
    query = f"""
        SELECT p.id, p.document_id, p.page_no, d.doknr
        FROM pages p
        JOIN documents d ON p.document_id = d.id
        WHERE length(p.text) > 300
          AND ({conditions})
        ORDER BY d.doknr, p.page_no
    """
    return conn.execute(query).fetchall()


def load_progress():
    """Laedt den Fortschritt (bereits verarbeitete page IDs)."""
    if os.path.exists(PROGRESS_PATH):
        with open(PROGRESS_PATH, encoding='utf-8') as f:
            return set(json.load(f))
    return set()


def save_progress(done_ids):
    """Speichert den Fortschritt."""
    os.makedirs(os.path.dirname(PROGRESS_PATH), exist_ok=True)
    with open(PROGRESS_PATH, 'w', encoding='utf-8') as f:
        json.dump(sorted(done_ids), f)


def ocr_page(pdf_path, page_no_1based):
    """Extrahiert Text einer PDF-Seite mit Tesseract OCR.

    page_no_1based: 1-indiziert (wie in der DB).
    Gibt den OCR-Text zurueck oder None bei Fehler.
    """
    doc = pymupdf.open(pdf_path)
    page = doc[page_no_1based - 1]

    # Seite als Bild rendern (100 DPI: schneller, Qualitaet reicht fuer Wortextraktion)
    pix = page.get_pixmap(dpi=100)
    img_path = f'/tmp/reocr_page_{os.getpid()}.png'
    pix.save(img_path)
    doc.close()

    out_base = f'/tmp/reocr_out_{os.getpid()}'
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


def reprocess_document(conn, document_id):
    """Verarbeitet die Woerter eines Dokuments neu.

    1. Loescht alte Woerter des Dokuments aus words + queue
    2. Baut Volltext aus pages zusammen
    3. Laesst text_parse drueberlaufen
    """
    from text_parse import process_woerter, prune

    # Alte Woerter dieses Dokuments entfernen
    conn.execute("DELETE FROM words WHERE document_id = ?", (document_id,))
    conn.execute("DELETE FROM queue WHERE document_id = ?", (document_id,))
    conn.commit()

    # Volltext aus pages zusammenbauen
    pages = conn.execute(
        "SELECT text FROM pages WHERE document_id = ? ORDER BY page_no",
        (document_id,)
    ).fetchall()
    full_text = '\n'.join(p['text'] for p in pages)

    if not full_text.strip():
        return 0

    new_words = process_woerter(full_text, document_id)
    if new_words:
        prune(new_words, document_id)
    flush()
    return len(new_words) if new_words else 0


def main():
    dry_run = "--dry-run" in sys.argv
    apply = "--apply" in sys.argv

    if not dry_run and not apply:
        print("Aufruf: uv run python utilities/reocr.py [--dry-run|--apply]")
        sys.exit(1)

    conn = _get_connection()
    garbled = find_garbled_pages(conn)
    logging.info(f"{len(garbled)} garbled Seiten gefunden")

    if dry_run:
        # Statistik zeigen
        docs = {}
        for row in garbled:
            doknr = row['doknr']
            docs.setdefault(doknr, []).append(row['page_no'])
        logging.info(f"{len(docs)} betroffene Dokumente:")
        for doknr in sorted(docs):
            pages = docs[doknr]
            logging.info(f"  {doknr}: {len(pages)} Seiten ({pages[0]}-{pages[-1]})")
        return

    # Apply mode: re-OCR + reprocess
    done_ids = load_progress()
    todo = [r for r in garbled if r['id'] not in done_ids]
    logging.info(f"{len(todo)} Seiten noch zu verarbeiten ({len(done_ids)} bereits erledigt)")

    affected_docs = set()
    start_time = time.time()

    for i, row in enumerate(todo):
        page_id = row['id']
        doc_id = row['document_id']
        doknr = row['doknr']
        page_no = row['page_no']

        # PDF-Pfad
        safe_doknr = doknr.replace('/', '-')
        pdf_path = os.path.join(ARCHIVE_DIR, f'{safe_doknr}.pdf')

        if not os.path.exists(pdf_path):
            logging.warning(f"PDF nicht gefunden: {pdf_path}")
            done_ids.add(page_id)
            continue

        logging.info(f"[{i+1}/{len(todo)}] OCR: {doknr} Seite {page_no}")
        ocr_text = ocr_page(pdf_path, page_no)

        if ocr_text is None:
            logging.warning(f"  OCR fehlgeschlagen, ueberspringe")
            done_ids.add(page_id)
            save_progress(done_ids)
            continue

        # Seite in DB aktualisieren
        conn.execute(
            "UPDATE pages SET text = ?, char_count = ? WHERE id = ?",
            (ocr_text, len(ocr_text), page_id)
        )
        conn.commit()
        affected_docs.add(doc_id)
        done_ids.add(page_id)

        # Fortschritt alle 10 Seiten speichern
        if (i + 1) % 10 == 0:
            save_progress(done_ids)
            elapsed = time.time() - start_time
            per_page = elapsed / (i + 1)
            remaining = per_page * (len(todo) - i - 1)
            logging.info(f"  Fortschritt: {i+1}/{len(todo)}, "
                        f"~{per_page:.0f}s/Seite, "
                        f"~{remaining/3600:.1f}h verbleibend")

    save_progress(done_ids)
    logging.info(f"OCR abgeschlossen: {len(todo)} Seiten verarbeitet")

    # Betroffene Dokumente neu verarbeiten
    logging.info(f"{len(affected_docs)} Dokumente werden neu verarbeitet...")
    for i, doc_id in enumerate(sorted(affected_docs)):
        doc = conn.execute("SELECT doknr FROM documents WHERE id = ?",
                          (doc_id,)).fetchone()
        doknr = doc['doknr'] if doc else f"ID {doc_id}"
        logging.info(f"[{i+1}/{len(affected_docs)}] Reprocess: {doknr}")
        new_count = reprocess_document(conn, doc_id)
        logging.info(f"  {new_count} neue Woerter")

    logging.info("Fertig!")


if __name__ == "__main__":
    main()
