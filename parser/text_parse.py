import logging
import json
import os
import re
from database import add_to_queue, check_newness

# Umlaut-Merge-Map laden (from_key -> canonical_key)
_MERGE_MAP_PATH = os.path.join(os.path.dirname(__file__), 'data', 'umlaut_merge_map.json')
_merge_map = {}
if os.path.exists(_MERGE_MAP_PATH):
    with open(_MERGE_MAP_PATH, encoding='utf-8') as f:
        _merge_map = json.load(f)
    logging.info(f'{len(_merge_map)} Umlaut-Merge-Regeln geladen')


def _suffix_normalize(word: str) -> str:
    """Normalisierung per Suffix-Regeln fuer group_key."""
    if word.endswith('innen') and len(word) > 6:
        return word[:-5]
    for suffix, cut in [('es', 2), ('en', 2), ('er', 2), ('se', 2),
                        ('ern', 3), ('s', 1), ('n', 1), ('e', 1)]:
        if word.endswith(suffix) and len(word) - cut >= 4:
            return word[:-cut]
    return word


# Beginn des Dokumentes finden mit Rechtschreibfehlern.
def find_beginn(text):

    if text.find('Beginn:') == -1:
        text = text[text.find('Beginn'):]
    else:
        text = text[text.find('Beginn'):]

    return text

# Silbentrennung rueckgaengig machen.
def dehyphenate(text):

    lines = text.split('\n')
    for num, line in enumerate(lines):
        stripped = line.rstrip()
        if stripped.endswith('-') or stripped.endswith('\u00b7') or stripped.endswith('\u2022'):
            try:
                next_words = lines[num+1].split()
                if not next_words:
                    continue
                # the end of the word is at the start of next line
                end = next_words[0]
                # we remove the - / · / • and append the end of the word
                lines[num] = stripped[:-1] + end
                # and remove the end of the word and possibly the
                # following space from the next line
                lines[num+1] = lines[num+1][lines[num+1].index(end) + len(end):]
            except (IndexError, ValueError):
                continue

    return '\n'.join(lines)

# Cleaning vor dem Wordsplitting
def pre_split_clean(text):

    regex_url = r'(http|ftp|https)://[\w_-]+(?:\.[\w_-]+)+[\w.,@?^=%&:/~+#-]*'
    text = re.sub(regex_url, '', text) # URL-Filter

    # Satzzeichen werden durch Leerzeichen ersetzt
    punctuation = """#"!$%&'()*+,\u201a.":;<=>?@[\\]^_`{|}~\u201c\u201d\u201e\u2018\u02bc"""
    for character in punctuation:
        text = text.replace(character, ' ')
    text = text.replace('\xa0', ' ') # NBSP entfernen
    text = text.replace('\u00b7', ' ') # Middle Dot entfernen (WP12 Silbentrennung)
    text = text.replace('\u2022', ' ') # Bullet entfernen
    text = text.replace('  ', ' ') # Doppelte Leerzeichen zu einfachen.

    return text

# Hauptfunktion des Moduls fuer die Aufbereitung und Trennung der Woerter
def process_woerter(text, document_id):

    if not text:
        return []

    # Verarbeitung des String
    text = find_beginn(text)
    text = dehyphenate(text)
    text = pre_split_clean(text)

    # Tokenisierung und Filterung
    words = text.split()
    new_words = []

    for word in words:
        # Aufzaehlungs-Artefakte ueberspringen
        if word.startswith('-') or word.endswith('-') or word.endswith('\u2013'):
            continue
        if not ok_word(word):
            continue
        group_key = _suffix_normalize(word.lower())
        group_key = _merge_map.get(group_key, group_key)
        if check_newness(word, group_key, document_id):
            new_words.append((word, group_key))

    return new_words


# Vokale (inkl. Umlaute) fuer Plausibilitaetspruefung
_VOWELS = set('aeiouAEIOUäöüÄÖÜ')

# Check ob es ein valides Wort ist
def ok_word(word):

    # Mindestlaenge 2 Zeichen
    if len(word) < 2:
        return False

    # Wort muss mindestens einen Vokal enthalten (jedes deutsche Wort hat Vokale)
    if not any(c in _VOWELS for c in word):
        return False

    # Wort hat gleiche Zeichen mehrmals hintereinander
    regmul = re.compile(r'([A-Za-z])\1{4,}')
    # Wort hat nicht nur am Anfang Grossbuchstaben
    regsmall = re.compile(r'[A-Za-z][a-z]*[A-Z]+[a-z]*')

    if regmul.search(word) or regsmall.search(word):
        return False

    return (not any(i.isdigit() or i in '(.@/#_\xa7 ' for i in word))

# Aussortieren von Woertern fuer Postings
def prune(new_words, document_id):

    pruned_words = find_matches(new_words)

    regcomp = re.compile(r'[a-z]+[-\u2013][a-z]+')
    for surface, group_key in pruned_words:
        if regcomp.search(surface) or len(surface) < 5:
            continue
        else:
            add_to_queue(surface, group_key, document_id)


# Deduplizierung per group_key
def find_matches(new_words):
    if len(new_words) < 2:
        return new_words

    seen = set()
    result = []
    for surface, group_key in new_words:
        if group_key not in seen:
            seen.add(group_key)
            result.append((surface, group_key))
    return result
