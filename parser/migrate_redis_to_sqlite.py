"""
One-time migration script: reads all data from the three Redis databases
and writes it into the new SQLite database.

Usage:
    cd parser && python migrate_redis_to_sqlite.py
"""

import redis
import time
from database import (
    init_db, add_to_database, add_protocol, add_to_queue,
    set_meta, _get_connection, DB_PATH
)


def migrate():
    # Redis connections matching the old database.py
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    postRedis = redis.StrictRedis(host='localhost', port=6379, db=1)
    pastRedis = redis.StrictRedis(host='localhost', port=6379, db=2)

    # Ensure SQLite tables exist
    init_db()

    conn = _get_connection()

    # --- Migrate words (db=0, keys matching word:*) ---
    word_count = 0
    for key in r.scan_iter('word:*'):
        data = r.hgetall(key)
        word = data.get(b'word', b'').decode('utf-8')
        word_id = data.get(b'id', b'').decode('utf-8')
        if word and word_id:
            conn.execute(
                "INSERT OR REPLACE INTO words (word, protocol_id) VALUES (?, ?)",
                (word, int(word_id))
            )
            word_count += 1
    conn.commit()
    print(f"Migrated {word_count} words")

    # --- Migrate protocols (db=0, keys matching protokoll:*) ---
    protocol_count = 0
    for key in r.scan_iter('protokoll:*'):
        protocol_id = key.decode('utf-8').split(':', 1)[1]
        data = r.hgetall(key)

        fields = {}
        for field_name in ['dokumentnummer', 'wahlperiode', 'protokollnummer', 'datum', 'titel', 'pdf_url']:
            val = data.get(field_name.encode('utf-8'))
            if val is not None:
                fields[field_name] = val.decode('utf-8')

        conn.execute(
            """INSERT OR REPLACE INTO protocols (id, dokumentnummer, wahlperiode, protokollnummer, datum, titel, pdf_url)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                int(protocol_id),
                fields.get('dokumentnummer'),
                int(fields['wahlperiode']) if 'wahlperiode' in fields else None,
                int(fields['protokollnummer']) if 'protokollnummer' in fields else None,
                fields.get('datum'),
                fields.get('titel'),
                fields.get('pdf_url'),
            )
        )
        protocol_count += 1
    conn.commit()
    print(f"Migrated {protocol_count} protocols")

    # --- Migrate meta:id (db=0) ---
    meta_id = r.get('meta:id')
    if meta_id is not None:
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value, expires_at) VALUES (?, ?, ?)",
            ('current_id', meta_id.decode('utf-8'), None)
        )
        print(f"Migrated meta:id = {meta_id.decode('utf-8')} as 'current_id'")

    # --- Migrate meta:tweetstop → poststop with TTL (db=1) ---
    poststop = postRedis.get('meta:tweetstop')
    if poststop is not None:
        ttl = postRedis.ttl('meta:tweetstop')
        expires_at = time.time() + ttl if ttl > 0 else None
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value, expires_at) VALUES (?, ?, ?)",
            ('poststop', poststop.decode('utf-8'), expires_at)
        )
        print(f"Migrated meta:tweetstop as 'poststop' (TTL={ttl}s)")

    conn.commit()

    # --- Migrate queue entries (db=1) ---
    queue_count = 0
    for key in postRedis.scan_iter('*'):
        key_str = key.decode('utf-8')
        if key_str.startswith('meta:'):
            continue
        data = postRedis.hgetall(key)
        word = data.get(b'word', b'').decode('utf-8')
        word_id = data.get(b'id', b'').decode('utf-8')
        if word and word_id:
            conn.execute(
                "INSERT OR REPLACE INTO queue (word, protocol_id) VALUES (?, ?)",
                (word, int(word_id))
            )
            queue_count += 1
    conn.commit()
    print(f"Migrated {queue_count} queue entries")

    # --- Migrate archive entries (db=2) ---
    archive_count = 0
    for key in pastRedis.scan_iter('*'):
        key_str = key.decode('utf-8')
        data = pastRedis.hgetall(key)
        mastodon_id = data.get(b'mastodon_id', b'').decode('utf-8')
        conn.execute(
            "INSERT OR REPLACE INTO archive (word, mastodon_id) VALUES (?, ?)",
            (key_str, mastodon_id if mastodon_id else None)
        )
        archive_count += 1
    conn.commit()
    print(f"Migrated {archive_count} archive entries")

    conn.close()

    # --- Verification ---
    print(f"\nSQLite database written to: {DB_PATH}")
    print("\nVerification:")
    print(f"  Redis db=0 word:* keys:     {len(list(r.scan_iter('word:*')))}")
    print(f"  Redis db=0 protokoll:* keys: {len(list(r.scan_iter('protokoll:*')))}")
    print(f"  Redis db=1 keys:             {postRedis.dbsize()}")
    print(f"  Redis db=2 keys:             {pastRedis.dbsize()}")

    verify_conn = _get_connection()
    for table in ['words', 'protocols', 'meta', 'queue', 'archive']:
        count = verify_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  SQLite {table}: {count}")
    verify_conn.close()


if __name__ == '__main__':
    migrate()
