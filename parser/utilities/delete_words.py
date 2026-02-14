from database import get_queue_size, get_random_queue_word, delete_from_queue, _get_connection


# Entfernt alle Wörter aus der Queue und der Datenbank

while get_queue_size() > 0:
    entry = get_random_queue_word()
    if entry is None:
        break

    word = entry['word']

    # Aus der Wort-Datenbank entfernen
    conn = _get_connection()
    conn.execute("DELETE FROM words WHERE word = ?", (word,))
    conn.commit()
    conn.close()

    # Aus der Queue entfernen
    delete_from_queue(word)
