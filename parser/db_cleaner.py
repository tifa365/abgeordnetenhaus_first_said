from database import get_queue_size, get_random_queue_word, delete_from_queue


total_keys = get_queue_size()

if total_keys > 400:
    remove = total_keys - 400

    for i in range(0, remove):
        entry = get_random_queue_word()
        if entry is None:
            break
        delete_from_queue(entry['word'])
