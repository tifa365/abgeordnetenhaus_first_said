import logging
from database import get_meta, set_meta, get_document, get_random_queue_word, move_to_archive, delete_from_queue
from mastodon_cred import toot_word
import random
from dotenv import load_dotenv

load_dotenv()


# Organisiert das Senden von Posts
def post_from_queue():

    poststop = get_meta('poststop')

    if poststop is None:
        set_post_stopper()
        logging.info('Post Skript wird gestartet')
        entry = get_random_queue_word()

        if entry is None:
            logging.info('Keine Woerter in der Queue.')
            return False

        word = entry['word']
        document_id = entry['document_id']
        logging.info("Wort '" + word + "' wird veroeffentlicht.")

        doc_keys = get_document(document_id)

        if send_word(word, doc_keys):
            return True
        else:
            logging.debug('Wort konnte nicht gesendet werden.')
            delete_from_queue(word)
            return False

    else:
        return False


def send_word(word, keys):

    mastodon_id = toot_word(word, keys)

    if not mastodon_id:
        logging.debug('Es wurde keine Mastodon ID gefunden.')

    if mastodon_id:
        return cleanup_db(word, mastodon_id)
    else:
        raise Exception('Es wurde keine ID gefunden.')

def cleanup_db(word, mastodon_id):

    # Ins Archiv bewegen
    try:
        move_to_archive(word, mastodon_id)
        logging.info('Wort wurde ins Archiv verschoben.')
        return True
    except Exception as e:
        logging.exception(e)
        return False

def set_post_stopper():

    expireTime = 60*round(random.randrange(55,120))
    set_meta('poststop', 1, ex=expireTime)
    logging.info('Post-Stopper wurde gesetzt auf ' + str(expireTime/60) + ' Minuten.')

    return True




if __name__ == "__main__":
    import os
    log_file = os.path.join(os.path.dirname(__file__), 'post.log')
    logging.basicConfig(
        filename=log_file,
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
    post_from_queue()
