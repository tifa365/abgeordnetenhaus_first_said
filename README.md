# Plenum First Said


Plenum First Said ist ein Bot, der neue Wörter postet, die zum ersten Mal während einer Plenarsitzung des Berliner Abgeordnetenhauses gesagt wurden. Es wird in keiner Weise Korrektheit garantiert.

Das Projekt wurde durch [@NYT_first_said](https://github.com/MaxBittker/nyt-first-said) von Max Bittker inspiriert und dessen [Code](https://github.com/MaxBittker/nyt-first-said) als Startpunkt genutzt, jedoch zum großen Teil verändert.

## Funktionsweise

Über das **PARDOK**-System des Berliner Abgeordnetenhauses werden die Plenarprotokolle aller Wahlperioden (WP 11–19, seit 1989) abgerufen. Die XML-Metadaten unter `https://www.parlament-berlin.de/opendata/pardok-wp{N}.xml` werden täglich auf neue Protokolle geprüft. Wird ein neues Plenarprotokoll gefunden, wird das zugehörige PDF heruntergeladen und seitenweise mit PyMuPDF extrahiert. Jedes Wort wird mit einer selbsterstellten Datenbank abgeglichen. Sollte das Wort nicht in der Datenbank gefunden werden, wird es zu einer Warteschlange hinzugefügt und zu einem bestimmten Zeitpunkt gepostet. Der Account [@FSBT_Kontext](https://mastodon.social/@FSBT_Kontext) antwortet automatisiert auf jeden Post mit dem Plenarprotokoll-Verweis und PDF-Link.

Unregelmäßigkeiten entstehen z.B. durch Silbentrennungen, die nicht gut von Wortverbindungen getrennt werden können (z.B. Know- (neue Zeile) how) und Rechtschreibfehlern.

## Architektur

`plenar.py` ist die Hauptfunktion, die den Rest orchestriert. Sie wird stündlich aufgerufen und iteriert über alle konfigurierten Wahlperioden. `database.py` erlaubt eine Verbindung zur lokalen SQLite-Datenbank.

`pardok.py` lädt die PARDOK-XML-Dateien herunter und extrahiert die Metadaten der Plenarprotokolle. `pdf_extract.py` lädt die PDFs herunter und extrahiert den Text seitenweise mit PyMuPDF.

`post_queue.py` und `mastodon_cred.py` packt neue Wörter in eine Warteliste und postet diese in unterschiedlichen Zeitintervallen auf Mastodon.

`text_parse.py` ist für die Worttrennung und Normalisierung da, sowie die Verbindung zum Abgleich mit der Datenbank über `database.py`.

`api_functions.py` hilft bei der Abfrage externer URLs mit Retry-Logik.

Im Ordner utilities finden sich Skripte, die bei der Wartung der Datenbank helfen.

Über das Paket [python-dotenv](https://github.com/theskumar/python-dotenv) werden API-Schlüssel durch Umgebungsvariablen bereitgestellt. Dazu muss eine `.env` Datei in der Basis des Projektes existieren. In dem Repo liegt die Datei `example.env`, die alle Variabeln aufzählt.

## PARDOK Open Data

Das Berliner Abgeordnetenhaus stellt über PARDOK die Plenarprotokolle als XML-Metadaten und PDF-Volltexte zur Verfügung:

- XML-Metadaten: `https://www.parlament-berlin.de/opendata/pardok-wp{N}.xml` (eine Datei pro Wahlperiode, tägliche Updates)
- PDF-Volltexte: Verlinkt über `<LokURL>` in den XML-Dokumenten

Die XML-Dateien enthalten `<Vorgang>`-Elemente mit verschachtelten `<Dokument>`-Elementen. Plenarprotokolle werden durch `<DokArt>PlPr</DokArt>` identifiziert.

## Mastodon
Für Mastodon benutze ich [Mastodon.py](https://github.com/halcy/Mastodon.py). Dort gibt es auch eine Dokumentation, wie man die Keys richtig erstellt.

Die Mastodon Accounts findet man je unter <a rel="me" href="https://mastodon.social/@BT_First_Said">@BT_First_Said@mastodon.social</a> und <a rel="me" href="https://mastodon.social/@FSBT_Kontext">@FSBT_Kontext@mastodon.social</a>.


## Was bedeutet "neues Wort"?

Aus Gründen der Unterhaltung werden einige Worte aussortiert, die zwar tatsächlich zum ersten Mal so gesagt werden, aber nur bedingt an sich einen Informationswert haben. Folgendes wird z.B. versucht, herauszufiltern:
- Plural
- Genitiv
- gegenderte Formen
- Wörter unter 4 Buchstaben
- Gesetzesabkürzungen

Einige Schwierigkeiten machen hier immer noch die Flexion von Wörtern. Grundregeln der Grammatik sind als Filter hardgecodet, jedoch werden dadurch nicht alle Begriffe erfasst. Lemmatization-Pakete wie HanTa, Spacy und Simplemma kommmen mit Neologismen oder eher seltenen Wörtern wie 'Buttersäureanschläge' nicht wirklich zurecht.

## Abhängigkeiten

- Python >=3.10
- [uv](https://docs.astral.sh/uv/) als Paketmanager (`uv sync` zum Installieren)
- PyMuPDF (`pymupdf`) für PDF-Textextraktion

## TODOs
- [X] Sprecher:in im Kontext mit erwähnen ([#5](https://github.com/ungeschneuer/plenum_first_said/pull/5))
- [X] Weitere Verfeinerung der Wort-Normalisierung ([#8](https://github.com/ungeschneuer/plenum_first_said/pull/8)).

## Lizenz und Danksagung

Das Projekt steht unter der [GNU General Public License 3](https://www.gnu.org/licenses/gpl-3.0.de.html).
Das Projekt entsteht im Rahmen eines Nachwuchs-Stipendiums des [Bayerische Staatministeriums für Wissenschaft und Kunst](https://www.stmwk.bayern.de/) zur Förderung der künstlerischen Entfaltung. Außerdem noch ein großes Danke an [jk](https://github.com/hejjoe), der meinen Bot deutlich aufmerksamer beobachtet als ich selbst.



