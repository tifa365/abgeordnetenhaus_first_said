"""Qualitaetsfilter: Erkennt und entfernt Nicht-Woerter aus words und queue.

Identifiziert OCR-Artefakte, Zeichensalat und Fragmente anhand von
Heuristiken (Vokalanteil, Sonderzeichen, unmögliche Konsonantenfolgen).

Aufruf:
    cd parser && uv run python utilities/word_quality.py --dry-run
    cd parser && uv run python utilities/word_quality.py --apply
"""

import sys
import os
import re
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from database import _get_connection

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

VOWELS = set('aeiouäöüyAEIOUÄÖÜY')


def vowel_ratio(word):
    """Anteil Vokale im Wort (0.0 - 1.0)."""
    letters = [c for c in word if c.isalpha()]
    if not letters:
        return 0.0
    return sum(1 for c in letters if c in VOWELS) / len(letters)


def has_special_chars(word):
    """Enthaelt Mittelzeichen-Artefakte."""
    return '·' in word or '•' in word


def has_impossible_cluster(word):
    """Enthaelt Konsonantenfolgen die im Deutschen nicht vorkommen.

    5+ Konsonanten in Folge ohne Vokal dazwischen sind fast immer Artefakte
    (selbst 'Herbstschnee' hat nur rbstschn = 7, aber das ist ein Kompositum
    mit Vokal-Kontext). Wir pruefen nur reine Buchstabenketten.
    """
    # Nur Buchstaben betrachten (Bindestriche etc. ignorieren)
    letters_only = re.sub(r'[^a-zA-ZäöüÄÖÜß]', ' ', word)
    for part in letters_only.split():
        consonant_run = 0
        for c in part.lower():
            if c in 'aeiouäöüy':
                consonant_run = 0
            else:
                consonant_run += 1
                if consonant_run >= 7:
                    return True
    return False


def is_garbage(word):
    """Prueft ob ein Wort definitiv Muell ist. Gibt (bool, grund) zurueck."""
    # Sonderzeichen-Artefakte
    if has_special_chars(word):
        return True, 'sonderzeichen'

    # Nur Buchstaben und Bindestrich sind erlaubt fuer weitere Pruefung
    clean = re.sub(r'[^a-zA-ZäöüÄÖÜß-]', '', word)
    if len(clean) < 4:
        return False, None

    # Keine Vokale (bei 5+ Buchstaben)
    if len(clean) >= 5 and vowel_ratio(clean) == 0:
        return True, 'keine_vokale'

    # Extrem niedriger Vokalanteil (bei laengeren Woertern)
    if len(clean) >= 8 and vowel_ratio(clean) < 0.1:
        return True, 'vokalanteil_unter_10'

    # Unmögliche Konsonantenfolgen
    if len(clean) >= 6 and has_impossible_cluster(clean):
        return True, 'konsonantenfolge'

    return False, None


def scan_table(conn, table):
    """Scannt eine Tabelle und gibt Liste von (word, grund) Tupeln zurueck."""
    rows = conn.execute(f"SELECT word FROM {table}").fetchall()
    garbage = []
    for row in rows:
        word = row['word']
        is_bad, reason = is_garbage(word)
        if is_bad:
            garbage.append((word, reason))
    return garbage


def main():
    dry_run = "--dry-run" in sys.argv
    apply = "--apply" in sys.argv

    if not dry_run and not apply:
        print("Aufruf: uv run python utilities/word_quality.py [--dry-run|--apply]")
        sys.exit(1)

    conn = _get_connection()

    # Beide Tabellen scannen
    for table in ('queue', 'words'):
        logging.info(f"Scanne {table}...")
        before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        garbage = scan_table(conn, table)

        # Statistik nach Grund
        reasons = {}
        for word, reason in garbage:
            reasons[reason] = reasons.get(reason, 0) + 1

        logging.info(f"  {table}: {len(garbage)} von {before} Eintraegen sind Muell")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            logging.info(f"    {reason}: {count}")

        # Beispiele zeigen
        logging.info(f"  Beispiele:")
        for word, reason in garbage[:20]:
            logging.info(f"    {word:30s}  ({reason})")

        if apply and garbage:
            words_to_delete = [w for w, _ in garbage]
            conn.executemany(
                f"DELETE FROM {table} WHERE word = ?",
                [(w,) for w in words_to_delete]
            )
            conn.commit()
            after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logging.info(f"  {table}: {before} -> {after} (-{before - after})")
        elif dry_run:
            logging.info(f"  Trockenlauf -- keine Aenderungen.")


if __name__ == "__main__":
    main()
