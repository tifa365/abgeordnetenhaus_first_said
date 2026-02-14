import sqlite3
import logging
import os
import time
from datetime import datetime

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
        CREATE TABLE IF NOT EXISTS documents (
            id              INTEGER PRIMARY KEY,
            wp              INTEGER NOT NULL,
            doknr           TEXT NOT NULL,
            doc_date        TEXT NOT NULL,
            titel           TEXT,
            pdf_url         TEXT NOT NULL,
            pdf_sha256      TEXT,
            extract_status  TEXT NOT NULL DEFAULT 'pending',
            extract_method  TEXT,
            extracted_at    TEXT,
            error           TEXT,
            UNIQUE(doknr)
        );

        CREATE TABLE IF NOT EXISTS pages (
            id          INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            page_no     INTEGER NOT NULL,
            text        TEXT NOT NULL,
            char_count  INTEGER,
            UNIQUE(document_id, page_no)
        );

        CREATE TABLE IF NOT EXISTS words (
            word        TEXT PRIMARY KEY,
            document_id INTEGER NOT NULL REFERENCES documents(id)
        );

        CREATE TABLE IF NOT EXISTS queue (
            word        TEXT PRIMARY KEY,
            document_id INTEGER NOT NULL REFERENCES documents(id)
        );

        CREATE TABLE IF NOT EXISTS archive (
            word        TEXT PRIMARY KEY,
            mastodon_id TEXT
        );

        CREATE TABLE IF NOT EXISTS meta (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            expires_at REAL
        );
    """)

    # FTS5 und Trigger separat (CREATE VIRTUAL TABLE nicht in executescript mit IF NOT EXISTS)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pages_fts'")
    if cur.fetchone() is None:
        conn.execute("""
            CREATE VIRTUAL TABLE pages_fts
            USING fts5(text, content=pages, content_rowid=id, tokenize='unicode61')
        """)

    # Trigger fuer FTS-Synchronisation
    for trigger_sql in [
        """CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
            INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
        END""",
        """CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
            INSERT INTO pages_fts(pages_fts, rowid, text) VALUES ('delete', old.id, old.text);
        END""",
        """CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
            INSERT INTO pages_fts(pages_fts, rowid, text) VALUES ('delete', old.id, old.text);
            INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
        END""",
    ]:
        conn.execute(trigger_sql)

    conn.commit()
    conn.close()

# Tabellen beim Import erstellen
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


# --- documents ---

def add_document(doknr, wp, doc_date, titel, pdf_url):
    conn = _get_connection()
    cursor = conn.execute(
        """INSERT OR IGNORE INTO documents (doknr, wp, doc_date, titel, pdf_url)
           VALUES (?, ?, ?, ?, ?)""",
        (doknr, wp, doc_date, titel, pdf_url)
    )
    conn.commit()
    doc_id = cursor.lastrowid
    if doc_id == 0:
        row = conn.execute("SELECT id FROM documents WHERE doknr = ?", (doknr,)).fetchone()
        doc_id = row['id']
    conn.close()
    return doc_id

def document_exists(doknr):
    conn = _get_connection()
    row = conn.execute("SELECT 1 FROM documents WHERE doknr = ?", (doknr,)).fetchone()
    conn.close()
    return row is not None

def get_document(document_id):
    conn = _get_connection()
    row = conn.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
    conn.close()
    if row is None:
        return {}
    return dict(row)

def get_document_by_doknr(doknr):
    conn = _get_connection()
    row = conn.execute("SELECT * FROM documents WHERE doknr = ?", (doknr,)).fetchone()
    conn.close()
    if row is None:
        return {}
    return dict(row)

def update_extract_status(document_id, status, method=None, error=None):
    conn = _get_connection()
    conn.execute(
        """UPDATE documents
           SET extract_status = ?, extract_method = ?, extracted_at = ?, error = ?
           WHERE id = ?""",
        (status, method, datetime.now().isoformat() if status != 'pending' else None, error, document_id)
    )
    conn.commit()
    conn.close()


# --- pages ---

def add_page(document_id, page_no, text):
    char_count = len(text)
    conn = _get_connection()
    conn.execute(
        "INSERT OR REPLACE INTO pages (document_id, page_no, text, char_count) VALUES (?, ?, ?, ?)",
        (document_id, page_no, text, char_count)
    )
    conn.commit()
    conn.close()


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


def check_newness(word, document_id):
    conn = _get_connection()
    row = conn.execute("SELECT document_id FROM words WHERE word = ?", (word,)).fetchone()
    conn.close()

    if row is not None:
        check_age(word, document_id)
        return False

    if similiar_word(word):
        add_to_database(word, document_id)
        return False
    else:
        add_to_database(word, document_id)
        return True


def add_to_database(word, document_id):
    try:
        conn = _get_connection()
        conn.execute(
            "INSERT OR REPLACE INTO words (word, document_id) VALUES (?, ?)",
            (word, document_id)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.exception(e)
        raise


def check_age(word, document_id):
    conn = _get_connection()
    row = conn.execute("SELECT document_id FROM words WHERE word = ?", (word,)).fetchone()
    if row is None:
        conn.close()
        return False

    aktuelle_doc_id = row['document_id']
    if document_id == aktuelle_doc_id:
        conn.close()
        return False

    try:
        aktuell_doc = get_document(aktuelle_doc_id)
        neu_doc = get_document(document_id)

        # ISO-Datumsvergleich: frueheres Datum gewinnt
        if aktuell_doc.get('doc_date', '') > neu_doc.get('doc_date', ''):
            conn_update = _get_connection()
            conn_update.execute("UPDATE words SET document_id = ? WHERE word = ?", (document_id, word))
            conn_update.commit()
            conn_update.close()
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

def add_to_queue(word, document_id):
    if word[0].islower():
        return False

    conn = _get_connection()
    conn.execute(
        "INSERT OR REPLACE INTO queue (word, document_id) VALUES (?, ?)",
        (word, document_id)
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
    row = conn.execute("SELECT word, document_id FROM queue ORDER BY RANDOM() LIMIT 1").fetchone()
    conn.close()
    if row is None:
        return None
    return {'word': row['word'], 'document_id': row['document_id']}


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
