import logging
import hashlib
import os
from datetime import datetime
import pymupdf
from api_functions import get_url_content
from database import add_page, update_extract_status

ARCHIVE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'archive')


def _safe_filename(doknr):
    """DokNr wie '19/2' in sicheren Dateinamen umwandeln: '19-2'."""
    return doknr.replace('/', '-')


def download_pdf(url, doknr):
    """PDF herunterladen und im Archiv speichern. Gibt (Pfad, SHA256) zurueck."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    filename = _safe_filename(doknr) + '.pdf'
    filepath = os.path.join(ARCHIVE_DIR, filename)

    if os.path.exists(filepath):
        logging.info(f'PDF bereits vorhanden: {filepath}')
        sha256 = hashlib.sha256(open(filepath, 'rb').read()).hexdigest()
        return filepath, sha256

    logging.info(f'Lade PDF: {url}')
    response = get_url_content(url)

    if response is None or response.status_code != 200:
        logging.warning(f'PDF konnte nicht geladen werden: {url} (Status: {response.status_code if response else "None"})')
        return None, None

    with open(filepath, 'wb') as f:
        f.write(response.content)

    sha256 = hashlib.sha256(response.content).hexdigest()
    logging.info(f'PDF gespeichert: {filepath} ({len(response.content)} Bytes)')
    return filepath, sha256


def extract_pages(pdf_path):
    """PDF seitenweise mit PyMuPDF extrahieren.

    Gibt Liste von (page_no, text, char_count) Tupeln zurueck (1-indiziert).
    """
    pages = []
    doc = pymupdf.open(pdf_path)

    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        pages.append((page_num + 1, text, len(text)))

    doc.close()
    return pages


def process_document(document_id, pdf_url, doknr):
    """PDF herunterladen, extrahieren und Seiten in DB speichern.

    Gibt den vollen Text als String zurueck (alle Seiten zusammen),
    oder None bei Fehler.
    """
    try:
        filepath, sha256 = download_pdf(pdf_url, doknr)
        if filepath is None:
            update_extract_status(document_id, 'failed', error='Download fehlgeschlagen')
            return None

        # SHA256 in der DB speichern
        from database import _get_connection
        conn = _get_connection()
        conn.execute("UPDATE documents SET pdf_sha256 = ? WHERE id = ?", (sha256, document_id))
        conn.commit()
        conn.close()

        pages = extract_pages(filepath)

        if not pages:
            update_extract_status(document_id, 'empty', method='pymupdf')
            return None

        full_text_parts = []
        for page_no, text, char_count in pages:
            add_page(document_id, page_no, text)
            full_text_parts.append(text)

        update_extract_status(document_id, 'ok', method='pymupdf')
        logging.info(f'Dokument {doknr}: {len(pages)} Seiten extrahiert')
        return '\n'.join(full_text_parts)

    except Exception as e:
        logging.exception(f'Fehler bei Verarbeitung von {doknr}: {e}')
        update_extract_status(document_id, 'failed', method='pymupdf', error=str(e))
        return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    test_url = 'https://pardok.parlament-berlin.de/starweb/adis/citat/VT/19/PlenarPr/p19-002-wp.pdf'
    path, sha = download_pdf(test_url, '19/2')
    if path:
        pages = extract_pages(path)
        print(f'{len(pages)} Seiten extrahiert')
        for pno, text, cc in pages[:2]:
            print(f'  Seite {pno}: {cc} Zeichen')
