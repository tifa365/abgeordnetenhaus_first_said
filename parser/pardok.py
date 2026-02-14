import logging
import xml.etree.ElementTree as ET
from api_functions import get_url_content
from database import document_exists

PARDOK_URL = 'https://www.parlament-berlin.de/opendata/pardok-wp{}.xml'


def fetch_pardok_xml(wp):
    """XML-Datei einer Wahlperiode herunterladen und parsen."""
    url = PARDOK_URL.format(wp)
    logging.info(f'Lade PARDOK-XML fuer WP {wp}: {url}')
    response = get_url_content(url)

    if response is None or response.status_code != 200:
        logging.warning(f'PARDOK-XML fuer WP {wp} konnte nicht geladen werden (Status: {response.status_code if response else "None"})')
        return None

    root = ET.fromstring(response.content)
    return root


def _convert_date(date_str):
    """Datum von DD.MM.YYYY nach ISO YYYY-MM-DD konvertieren."""
    parts = date_str.strip().split('.')
    if len(parts) == 3:
        return f'{parts[2]}-{parts[1]}-{parts[0]}'
    return date_str


def _get_text(element, tag):
    """Text eines Kind-Elements holen, None wenn nicht vorhanden."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def extract_plpr_documents(root):
    """Alle eindeutigen Plenarprotokolle aus dem XML extrahieren.

    Gibt eine Liste von Dicts zurueck:
    {doknr, wp, doc_date, titel, pdf_url}
    """
    seen = {}

    for vorgang in root.iter('Vorgang'):
        for dokument in vorgang.iter('Dokument'):
            dokart = _get_text(dokument, 'DokArt')
            if dokart != 'PlPr':
                continue

            doknr = _get_text(dokument, 'DokNr')
            if not doknr or doknr in seen:
                continue

            pdf_url = _get_text(dokument, 'LokURL')
            if not pdf_url:
                continue

            doc_date_raw = _get_text(dokument, 'DokDat')
            if not doc_date_raw:
                continue

            wp = _get_text(dokument, 'Wp')
            titel = _get_text(dokument, 'Titel')

            seen[doknr] = {
                'doknr': doknr,
                'wp': int(wp) if wp else 0,
                'doc_date': _convert_date(doc_date_raw),
                'titel': titel,
                'pdf_url': pdf_url,
            }

    documents = sorted(seen.values(), key=lambda d: d['doc_date'])
    logging.info(f'{len(documents)} Plenarprotokolle in XML gefunden')
    return documents


def find_new_documents(wp):
    """Neue, noch nicht in der DB vorhandene Plenarprotokolle einer Wahlperiode finden."""
    root = fetch_pardok_xml(wp)
    if root is None:
        return []

    all_docs = extract_plpr_documents(root)
    new_docs = [d for d in all_docs if not document_exists(d['doknr'])]
    logging.info(f'{len(new_docs)} neue Protokolle fuer WP {wp}')
    return new_docs


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
    docs = find_new_documents(19)
    for d in docs[:5]:
        print(f"  {d['doknr']} | {d['doc_date']} | {d['titel']}")
    print(f'... {len(docs)} Protokolle insgesamt')
