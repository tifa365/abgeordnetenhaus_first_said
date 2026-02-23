# Abgeordnetenhaus First Said

Abgeordnetenhaus First Said ist ein Bot, der neue Wörter postet, die zum ersten Mal während einer Plenarsitzung des Berliner Abgeordnetenhauses gesagt wurden. Es wird in keiner Weise Korrektheit garantiert.

Das Projekt basiert auf [Plenum First Said](https://github.com/ungeschneuer/plenum_first_said) von ungeschneuer, das wiederum durch [@NYT_first_said](https://github.com/MaxBittker/nyt-first-said) von Max Bittker inspiriert wurde.

## Funktionsweise

Über das **PARDOK**-System des Berliner Abgeordnetenhauses werden die Plenarprotokolle aller Wahlperioden (WP 12–19, seit 1991) abgerufen. Die XML-Metadaten unter `https://www.parlament-berlin.de/opendata/pardok-wp{N}.xml` werden wöchentlich auf neue Protokolle geprüft. Wird ein neues Plenarprotokoll gefunden, wird das zugehörige PDF heruntergeladen und seitenweise mit PyMuPDF extrahiert. Jedes Wort wird mit einer selbsterstellten Datenbank abgeglichen. Sollte das Wort nicht in der Datenbank gefunden werden, wird es zu einer Warteschlange hinzugefügt und alle 1–2 Stunden auf Mastodon gepostet.

Unregelmäßigkeiten entstehen z.B. durch Silbentrennungen, die nicht gut von Wortverbindungen getrennt werden können (z.B. Know- (neue Zeile) how) und Rechtschreibfehlern.

## Architektur

`plenar.py` ist die Hauptfunktion, die den Rest orchestriert. Sie wird wöchentlich per GitHub Actions aufgerufen und iteriert über alle konfigurierten Wahlperioden. `database.py` erlaubt eine Verbindung zur lokalen SQLite-Datenbank.

`pardok.py` lädt die PARDOK-XML-Dateien herunter und extrahiert die Metadaten der Plenarprotokolle. `pdf_extract.py` lädt die PDFs herunter und extrahiert den Text seitenweise mit PyMuPDF.

`post_queue.py` und `mastodon_cred.py` posten Wörter aus der Warteschlange auf Mastodon.

`text_parse.py` ist für die Worttrennung und Normalisierung da, sowie die Verbindung zum Abgleich mit der Datenbank über `database.py`.

`api_functions.py` hilft bei der Abfrage externer URLs mit Retry-Logik.

Im Ordner `utilities/` finden sich Skripte, die bei der Wartung der Datenbank helfen.

Über das Paket [python-dotenv](https://github.com/theskumar/python-dotenv) werden API-Schlüssel durch Umgebungsvariablen bereitgestellt. Dazu muss eine `.env` Datei in der Basis des Projektes existieren. In dem Repo liegt die Datei `example.env`, die alle Variabeln aufzählt.

## GitHub Actions

Der Bot läuft vollautomatisch über GitHub Actions:

- **`post.yml`**: Postet stündlich (8–22 Uhr) ein neues Wort aus der Warteschlange, mit einem Cooldown-Timer von 55–120 Minuten zwischen Posts.
- **`scan.yml`**: Prüft jeden Montag auf neue Plenarprotokolle, lädt PDFs herunter, extrahiert neue Wörter und fügt sie zur Warteschlange hinzu.

## PARDOK Open Data

Das Berliner Abgeordnetenhaus stellt über PARDOK die Plenarprotokolle als XML-Metadaten und PDF-Volltexte zur Verfügung:

- XML-Metadaten: `https://www.parlament-berlin.de/opendata/pardok-wp{N}.xml` (eine Datei pro Wahlperiode)
- PDF-Volltexte: Verlinkt über `<LokURL>` in den XML-Dokumenten

Die XML-Dateien enthalten `<Vorgang>`-Elemente mit verschachtelten `<Dokument>`-Elementen. Plenarprotokolle werden durch `<DokArt>PlPr</DokArt>` identifiziert.

Plenarsitzungen finden fast ausschließlich donnerstags statt, in der Regel alle zwei Wochen.

## Mastodon

Für Mastodon wird [Mastodon.py](https://github.com/halcy/Mastodon.py) verwendet.

Den Mastodon Account findet man unter <a rel="me" href="https://mastodon.social/@ab_first_said">@ab_first_said@mastodon.social</a>.

## Was bedeutet "neues Wort"?

Aus Gründen der Unterhaltung werden einige Worte aussortiert, die zwar tatsächlich zum ersten Mal so gesagt werden, aber nur bedingt an sich einen Informationswert haben. Folgendes wird z.B. herausgefiltert:

- Plural und Genitiv (per Suffix-Normalisierung)
- Gegenderte Formen (-innen)
- Wörter unter 5 Buchstaben
- Wörter ohne Vokale (OCR-Artefakte)
- Wörter mit mehr als 4 gleichen Buchstaben hintereinander
- Gesetzesabkürzungen und Zahlen

Die Flexion von Wörtern wird über handgeschriebene Suffix-Regeln und eine Umlaut-Merge-Map normalisiert. Lemmatization-Pakete wie spaCy kommen mit Neologismen oder seltenen Komposita wie "Buttersäureanschläge" nicht zurecht und sind für diesen Anwendungsfall zu langsam.

## Abhängigkeiten

- Python >=3.10
- [uv](https://docs.astral.sh/uv/) als Paketmanager (`uv sync` zum Installieren)
- PyMuPDF (`pymupdf`) für PDF-Textextraktion

## Lizenz und Danksagung

Das Projekt steht unter der [GNU General Public License 3](https://www.gnu.org/licenses/gpl-3.0.de.html). Basierend auf [Plenum First Said](https://github.com/ungeschneuer/plenum_first_said) von ungeschneuer. Ursprünglich inspiriert durch [NYT First Said](https://github.com/MaxBittker/nyt-first-said) von Max Bittker.
