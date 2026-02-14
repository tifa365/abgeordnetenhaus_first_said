# -*- coding: utf-8 -*-
import logging
import os
from dotenv import load_dotenv
from pardok import find_new_documents
from pdf_extract import process_document
from database import add_document
from text_parse import process_woerter, prune

load_dotenv()


def get_wahlperioden():
    """Wahlperioden aus Umgebungsvariable oder Default (11-19)."""
    env_val = os.environ.get('WAHLPERIODEN')
    if env_val:
        return [int(x.strip()) for x in env_val.split(',')]
    return list(range(11, 20))


def main():
    wahlperioden = get_wahlperioden()
    logging.info(f'Starte Suche fuer Wahlperioden: {wahlperioden}')

    for wp in wahlperioden:
        new_docs = find_new_documents(wp)

        for doc in new_docs:
            logging.info(f"Neues Protokoll gefunden: {doc['doknr']} vom {doc['doc_date']}")

            doc_id = add_document(
                doknr=doc['doknr'],
                wp=doc['wp'],
                doc_date=doc['doc_date'],
                titel=doc['titel'],
                pdf_url=doc['pdf_url'],
            )

            full_text = process_document(doc_id, doc['pdf_url'], doc['doknr'])

            if not full_text:
                logging.warning(f"Kein Text fuer {doc['doknr']} extrahiert.")
                continue

            new_words = process_woerter(full_text, doc_id)

            if not new_words:
                logging.debug(f"Keine neuen Woerter in {doc['doknr']}.")
                continue

            prune(new_words, doc_id)
            logging.info(f"{len(new_words)} neue Woerter in {doc['doknr']}.")

    logging.info('Suche abgeschlossen.')


if __name__ == "__main__":
    log_file = os.path.join(os.path.dirname(__file__), 'plenarlog.log')
    logging.basicConfig(
        filename=log_file,
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('Starte Plenar-Parser')
    main()
    logging.info('Beende Plenar-Parser')
