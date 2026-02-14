# -*- coding: utf-8 -*-
#!/usr/bin/python


import logging
from dotenv import load_dotenv
import os
from mastodon import Mastodon
import mastodon
from time import sleep

load_dotenv()


MastodonAPI = Mastodon(access_token = os.environ.get('MASTODON_FIRST_ACCESSTOKEN'),  api_base_url="https://mastodon.social")
MastodonKontextAPI = Mastodon(access_token = os.environ.get('MASTODON_KONTEXT_ACCESSTOKEN'),  api_base_url="https://mastodon.social")


# Mastodon API is a bit wobbly so a fix with while loops
def toot_word(word, keys):

    # Max tries to get posting trough
    patience = 0

    #Posts Word
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

    sleep(5)

    # Posts Context mit Berlin-Abgeordnetenhaus-Format
    patience = 0
    while True:
        if patience > 10:
            logging.info('Maximale Versuche wurde ueberschritten.')
            return False
        else:
            try:
                context_status = MastodonKontextAPI.status_post(
                    "#{} tauchte zum ersten Mal im Plenarprotokoll {} am {} im Berliner Abgeordnetenhaus auf.\n\nPDF: {}".format(
                        word,
                        keys.get('doknr', ''),
                        keys.get('doc_date', ''),
                        keys.get('pdf_url', '')),
                    in_reply_to_id=toot_status["id"])
            except mastodon.MastodonNotFoundError as m:
                logging.exception(m)
                sleep(60)
                patience += 1
                continue
            except Exception as e:
                logging.exception(e)
                sleep(60)
                patience += 1
                continue
            break


    logging.info('Toot wurde erfolgreich gesendet.')
    return toot_status["id"]
