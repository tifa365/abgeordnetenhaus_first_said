"""Reprocess: Alle Woerter neu extrahieren nach Pipeline-Fix.

Loescht alle words + queue und verarbeitet alle Dokumente neu.

Aufruf:
    cd parser && uv run python utilities/reprocess_all.py
"""

import sys
import os
import time
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database import _get_connection, flush
from text_parse import process_woerter, prune

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


def main():
    conn = _get_connection()

    # Alles loeschen
    conn.execute('DELETE FROM words')
    conn.execute('DELETE FROM queue')
    conn.commit()
    logging.info('Words und Queue geleert')

    # Alle Dokumente (WP11 hat keine Page-Texte, wird uebersprungen)
    docs = conn.execute(
        'SELECT id, wp, doknr FROM documents ORDER BY wp, doknr'
    ).fetchall()
    logging.info(f'{len(docs)} Dokumente total')

    start = time.time()
    total_new = 0
    current_wp = None
    wp_count = 0

    for i, doc in enumerate(docs):
        doc_id = doc['id']
        wp = doc['wp']
        doknr = doc['doknr']

        if wp != current_wp:
            if current_wp is not None:
                logging.info(f'  WP{current_wp} fertig: {wp_count} Woerter')
            current_wp = wp
            wp_count = 0
            logging.info(f'Verarbeite WP{wp}...')

        # Volltext zusammenbauen
        pages = conn.execute(
            'SELECT text FROM pages WHERE document_id = ? ORDER BY page_no',
            (doc_id,)
        ).fetchall()
        full_text = '\n'.join(p['text'] for p in pages)

        if not full_text.strip():
            continue

        new_words = process_woerter(full_text, doc_id)
        if new_words:
            prune(new_words, doc_id)
        flush()
        count = len(new_words) if new_words else 0
        total_new += count
        wp_count += count

        if (i + 1) % 50 == 0:
            elapsed = time.time() - start
            logging.info(
                f'  [{i+1}/{len(docs)}] {doknr}: {count} W, '
                f'gesamt {total_new}, {elapsed:.0f}s'
            )

    if current_wp is not None:
        logging.info(f'  WP{current_wp} fertig: {wp_count} Woerter')

    elapsed = time.time() - start
    logging.info(f'Reprocess fertig: {total_new} Woerter in {elapsed:.0f}s')

    # Statistik
    print()
    for wp_row in conn.execute(
        'SELECT DISTINCT wp FROM documents ORDER BY wp'
    ).fetchall():
        wp = wp_row['wp']
        wc = conn.execute(
            'SELECT COUNT(*) FROM words w '
            'JOIN documents d ON w.document_id = d.id WHERE d.wp = ?',
            (wp,)
        ).fetchone()[0]
        qc = conn.execute(
            'SELECT COUNT(*) FROM queue q '
            'JOIN documents d ON q.document_id = d.id WHERE d.wp = ?',
            (wp,)
        ).fetchone()[0]
        print(f'WP{wp:>2d}: {wc:>6d} words, {qc:>5d} queue')
    all_w = conn.execute('SELECT COUNT(*) FROM words').fetchone()[0]
    all_q = conn.execute('SELECT COUNT(*) FROM queue').fetchone()[0]
    print(f'Total: {all_w} words, {all_q} queue')


if __name__ == '__main__':
    main()
