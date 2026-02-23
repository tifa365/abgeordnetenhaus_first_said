# -*- coding: utf-8 -*-
#!/usr/bin/python


import logging
from dotenv import load_dotenv
import os
from mastodon import Mastodon
import mastodon
from time import sleep

load_dotenv()


MASTODON_INSTANCE = os.environ.get('MASTODON_INSTANCE', 'https://mastodon.social')

MastodonAPI = Mastodon(access_token = os.environ.get('MASTODON_FIRST_ACCESSTOKEN'),  api_base_url=MASTODON_INSTANCE, request_timeout=30)


def toot_word(word, keys):

    patience = 0

    while True:
        if patience > 10:
            logging.info('Maximale Versuche wurde ueberschritten.')
            return False
        else:
            try:
                toot_status = MastodonAPI.toot(word)
            except Exception as e:
                logging.exception(e)
                sleep(60)
                patience += 1
                continue
            break

    logging.info('Toot wurde erfolgreich gesendet.')
    return toot_status["id"]
