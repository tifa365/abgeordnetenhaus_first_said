import sqlite3
import logging
import os
import time

DB_PATH = os.environ.get('SQLITE_DB_PATH', os.path.join(os.path.dirname(__file__), 'plenum_first_said.db'))

def _get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = _get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS words (
            word        TEXT PRIMARY KEY,
            protocol_id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS protocols (
            id               INTEGER PRIMARY KEY,
            dokumentnummer   TEXT,
            wahlperiode      INTEGER,
            protokollnummer  INTEGER,
            datum            TEXT,
            titel            TEXT,
            pdf_url          TEXT
        );

        CREATE TABLE IF NOT EXISTS meta (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            expires_at REAL
        );

        CREATE TABLE IF NOT EXISTS queue (
            word        TEXT PRIMARY KEY,
            protocol_id INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS archive (
            word        TEXT PRIMARY KEY,
            mastodon_id TEXT
        );
    """)
    conn.close()

# Ensure tables exist on import
init_db()


# --- meta ---

def get_meta(key):
    conn = _get_connection()
    row = conn.execute("SELECT value, expires_at FROM meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        conn.close()
        return None
    if row['expires_at'] is not None and time.time() > row['expires_at']:
        conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        conn.commit()
        conn.close()
        return None
    conn.close()
    return row['value']

def set_meta(key, value, ex=None):
    expires_at = time.time() + ex if ex else None
    conn = _get_connection()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value, expires_at) VALUES (?, ?, ?)",
        (key, str(value), expires_at)
    )
    conn.commit()
    conn.close()


# --- protocols ---

def add_protocol(protocol_id, **fields):
    conn = _get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO protocols (id, dokumentnummer, wahlperiode, protokollnummer, datum, titel, pdf_url)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            protocol_id,
            fields.get('dokumentnummer'),
            fields.get('wahlperiode'),
            fields.get('protokollnummer'),
            fields.get('datum'),
            fields.get('titel'),
            fields.get('pdf_url'),
        )
    )
    conn.commit()
    conn.close()

def get_protocol(protocol_id):
    conn = _get_connection()
    row = conn.execute("SELECT * FROM protocols WHERE id = ?", (protocol_id,)).fetchone()
    conn.close()
    if row is None:
        return {}
    return dict(row)


# --- words ---

def similiar_word(word):
    variants = [
        word.lower(),
        word.capitalize(),
        word + 'er',
        word + 'n',
        word + 'en',
        word + 's',
        word + 'es',
        word + 'e',
    ]

    if word.endswith(('s', 'n', 'e')):
        variants.append(word[:-1])

    if word.endswith(('\u2019s', 'in', '\u2019n', 'er', 'en', 'es', 'se')):
        variants.append(word[:-2])

    if word.endswith('ern'):
        variants.append(word[:-3])

    if word.endswith('m'):
        variants.append(word[:-1] + 'n')

    if word.endswith('n'):
        variants.append(word[:-1] + 'm')

    if word.endswith('en'):
        variants.append(word[:-2] + 'er')
        variants.append(word[:-2] + 'e')
        variants.append(word[:-2] + 't')

    if word.endswith('innen'):
        variants.append(word[:-5])

    placeholders = ','.join('?' for _ in variants)
    conn = _get_connection()
    row = conn.execute(
        f"SELECT 1 FROM words WHERE word IN ({placeholders}) LIMIT 1",
        variants
    ).fetchone()
    conn.close()
    return row is not None


def check_newness(word, id):
    conn = _get_connection()
    row = conn.execute("SELECT protocol_id FROM words WHERE word = ?", (word,)).fetchone()
    conn.close()

    if row is not None:
        check_age(word, id)
        return False

    if similiar_word(word):
        add_to_database(word, id)
        return False
    else:
        add_to_database(word, id)
        return True


def add_to_database(word, id):
    try:
        conn = _get_connection()
        conn.execute(
            "INSERT OR REPLACE INTO words (word, protocol_id) VALUES (?, ?)",
            (word, id)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.exception(e)
        raise


def check_age(word, id):
    conn = _get_connection()
    row = conn.execute("SELECT protocol_id FROM words WHERE word = ?", (word,)).fetchone()
    if row is None:
        conn.close()
        return False

    aktuelle_id = row['protocol_id']
    if str(id) == str(aktuelle_id):
        conn.close()
        return False

    try:
        aktuell_p = get_protocol(aktuelle_id)
        aktuelle_periode = int(aktuell_p['wahlperiode'])
        aktuelle_protokollnummer = int(aktuell_p['protokollnummer'])

        neu_p = get_protocol(id)
        neue_periode = int(neu_p['wahlperiode'])
        neue_protokollnummer = int(neu_p['protokollnummer'])

        if (aktuelle_periode == neue_periode and aktuelle_protokollnummer > neue_protokollnummer) or (aktuelle_periode > neue_periode):
            conn.execute("UPDATE words SET protocol_id = ? WHERE word = ?", (id, word))
            conn.commit()
            conn.close()
            return True
        else:
            conn.close()
            return False
    except Exception as e:
        conn.close()
        logging.exception(e)
        raise


# --- queue ---

def add_to_queue(word, id):
    if word[0].islower():
        return False

    conn = _get_connection()
    conn.execute(
        "INSERT OR REPLACE INTO queue (word, protocol_id) VALUES (?, ?)",
        (word, id)
    )
    conn.commit()
    conn.close()
    return True


def delete_from_queue(word):
    conn = _get_connection()
    cursor = conn.execute("DELETE FROM queue WHERE word = ?", (word,))
    conn.commit()
    deleted = cursor.rowcount > 0
    conn.close()
    return deleted


def get_random_queue_word():
    conn = _get_connection()
    row = conn.execute("SELECT word, protocol_id FROM queue ORDER BY RANDOM() LIMIT 1").fetchone()
    conn.close()
    if row is None:
        return None
    return {'word': row['word'], 'id': row['protocol_id']}


def get_queue_size():
    conn = _get_connection()
    row = conn.execute("SELECT COUNT(*) as cnt FROM queue").fetchone()
    conn.close()
    return row['cnt']


# --- archive ---

def move_to_archive(word, mastodon_id):
    conn = _get_connection()
    try:
        conn.execute("INSERT OR REPLACE INTO archive (word, mastodon_id) VALUES (?, ?)", (word, mastodon_id))
        conn.execute("DELETE FROM queue WHERE word = ?", (word,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.exception(e)
        raise
    finally:
        conn.close()
