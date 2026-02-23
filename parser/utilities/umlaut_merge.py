"""Umlaut/ß-Merge: Findet group_keys die sich nur durch Umlaut/ß unterscheiden
und merged sie auf eine kanonische Form.

Aufruf:
    cd parser && uv run python utilities/umlaut_merge.py --dry-run
    cd parser && uv run python utilities/umlaut_merge.py --apply
"""

import sys
import os
import json
import logging
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from database import _get_connection

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
MERGE_MAP_PATH = os.path.join(DATA_DIR, 'umlaut_merge_map.json')
MERGE_PLAN_PATH = os.path.join(DATA_DIR, 'umlaut_merge_plan.json')

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def fold_umlaut(s):
    """Umlaute und ß falten fuer Kollisionserkennung."""
    return (s.replace('ä', 'a').replace('ö', 'o').replace('ü', 'u')
             .replace('ß', 'ss'))


def has_umlaut(s):
    """Prueft ob ein String Umlaute oder ß enthaelt."""
    return any(c in s for c in 'äöüß')


def pick_canonical(group_keys_with_counts):
    """Waehlt den kanonischen group_key aus einer Kollisionsgruppe.

    Regeln:
    1. Bevorzuge den Key MIT Umlaut/ß (korrekte deutsche Orthographie)
    2. Bei Gleichstand: haeufigster group_key
    3. Bei Gleichstand: lexikographisch kleinster
    """
    with_umlaut = [(k, c) for k, c in group_keys_with_counts if has_umlaut(k)]
    without_umlaut = [(k, c) for k, c in group_keys_with_counts if not has_umlaut(k)]

    # Bevorzuge Umlaut-Keys
    candidates = with_umlaut if with_umlaut else without_umlaut

    # Sortiere: haeufigster zuerst, dann lexikographisch
    candidates.sort(key=lambda x: (-x[1], x[0]))
    return candidates[0][0]


def build_merge_plan(conn):
    """Erstellt den Merge-Plan aus der words-Tabelle."""
    # Alle distinct group_keys mit Haeufigkeit laden
    rows = conn.execute("""
        SELECT group_key, COUNT(*) as cnt
        FROM words
        WHERE length(group_key) >= 5
        GROUP BY group_key
    """).fetchall()

    key_counts = {r['group_key']: r['cnt'] for r in rows}
    logging.info(f"{len(key_counts)} distinct group_keys geladen")

    # Nach gefaltetem Key gruppieren
    buckets = defaultdict(list)
    for key, count in key_counts.items():
        folded = fold_umlaut(key)
        buckets[folded].append((key, count))

    # Nur Kollisionsgruppen (> 1 verschiedene group_keys)
    collisions = {folded: members
                  for folded, members in buckets.items()
                  if len(members) > 1}
    logging.info(f"{len(collisions)} Umlaut-Kollisionsgruppen gefunden")

    # Merge-Map bauen
    merge_map = {}  # from_key -> to_key
    plan = []

    for folded, members in sorted(collisions.items()):
        canonical = pick_canonical(members)
        cluster = {
            'folded': folded,
            'canonical': canonical,
            'members': [{'key': k, 'count': c, 'merged': k != canonical}
                        for k, c in sorted(members)]
        }
        plan.append(cluster)

        for key, count in members:
            if key != canonical:
                merge_map[key] = canonical

    logging.info(f"{len(merge_map)} group_keys werden auf kanonische Form gemapped")
    return plan, merge_map


def apply_merges(conn, merge_map):
    """Wendet die Merges auf words und queue an."""
    # words aktualisieren
    updates_words = [(to_key, from_key) for from_key, to_key in merge_map.items()]
    conn.executemany(
        "UPDATE words SET group_key = ? WHERE group_key = ?",
        updates_words
    )
    words_changed = conn.execute("SELECT changes()").fetchone()[0]

    # queue aktualisieren
    conn.executemany(
        "UPDATE queue SET group_key = ? WHERE group_key = ?",
        updates_words
    )
    queue_changed = conn.execute("SELECT changes()").fetchone()[0]

    # Queue deduplizieren: bei gleicher group_key nur einen Eintrag behalten
    conn.execute("""
        DELETE FROM queue WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM queue GROUP BY group_key
        )
    """)
    queue_deduped = conn.execute("SELECT changes()").fetchone()[0]

    conn.commit()

    logging.info(f"words: {words_changed} Eintraege aktualisiert")
    logging.info(f"queue: {queue_changed} Eintraege aktualisiert, {queue_deduped} Duplikate entfernt")


def save_artifacts(plan, merge_map):
    """Speichert Plan und Map als JSON."""
    os.makedirs(DATA_DIR, exist_ok=True)

    with open(MERGE_PLAN_PATH, 'w', encoding='utf-8') as f:
        json.dump(plan, f, ensure_ascii=False, indent=2)
    logging.info(f"Merge-Plan gespeichert: {MERGE_PLAN_PATH}")

    with open(MERGE_MAP_PATH, 'w', encoding='utf-8') as f:
        json.dump(merge_map, f, ensure_ascii=False, indent=2, sort_keys=True)
    logging.info(f"Merge-Map gespeichert: {MERGE_MAP_PATH}")


def main():
    dry_run = "--dry-run" in sys.argv
    apply = "--apply" in sys.argv

    if not dry_run and not apply:
        print("Aufruf: uv run python utilities/umlaut_merge.py [--dry-run|--apply]")
        sys.exit(1)

    conn = _get_connection()

    # Statistik vorher
    queue_before = conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    distinct_keys_before = conn.execute(
        "SELECT COUNT(DISTINCT group_key) FROM words WHERE length(group_key) >= 4"
    ).fetchone()[0]

    plan, merge_map = build_merge_plan(conn)
    save_artifacts(plan, merge_map)

    # Beispiele zeigen
    logging.info("Beispiele:")
    for cluster in plan[:15]:
        members_str = " / ".join(
            f"{m['key']}({m['count']})" for m in cluster['members']
        )
        logging.info(f"  -> {cluster['canonical']:20s} aus: {members_str}")

    if apply:
        apply_merges(conn, merge_map)

        distinct_keys_after = conn.execute(
            "SELECT COUNT(DISTINCT group_key) FROM words WHERE length(group_key) >= 4"
        ).fetchone()[0]
        queue_after = conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]

        logging.info(f"Distinct group_keys: {distinct_keys_before} -> {distinct_keys_after} "
                     f"(-{distinct_keys_before - distinct_keys_after})")
        logging.info(f"Queue: {queue_before} -> {queue_after} "
                     f"(-{queue_before - queue_after})")
    else:
        logging.info("Trockenlauf -- keine Aenderungen. Mit --apply ausfuehren.")


if __name__ == "__main__":
    main()
