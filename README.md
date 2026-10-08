# MIRA – Guida semplice agli script per i dati sull'immigrazione


**Non occorre conoscere Python per capire i risultati.** Per avviare gli strumenti, invece, bisogna inserire alcuni comandi nel Terminale del computer.

## I due programmi: quale usare?

Conserviamo entrambi questi file nella stessa cartella:

| File | A cosa serve |
| --- | --- |
| `mira_pipeline.py` | Raccoglie e organizza i dati, controlla i file e crea tabelle e note. |
| `mira_pipeline_grafici.py` | Fa le stesse operazioni e, in più, crea grafici delle serie storiche. |

Il secondo **non sostituisce** il primo: è la versione con la funzione grafica aggiuntiva. I due programmi leggono gli stessi dati, se si trovano nella stessa cartella e si usa l'impostazione predefinita.

## 1. Preparazione (solo la prima volta)

Serve **Python 3.10 o successivo**. Aprire il **Terminale**, entrare nella cartella dove sono stati salvati i due file e controllare che Python sia disponibile:

```bash
python3 --version
```

Per la versione con i grafici serve anche una piccola libreria, chiamata `matplotlib`. Installarla con:

```bash
python3 -m pip install matplotlib
```

Il programma base utilizza soltanto gli strumenti inclusi in Python.

> **Nota per chi usa il Mac:** per entrare nella cartella, nel Terminale scrivere `cd ` (con uno spazio dopo `cd`), trascinare la cartella dal Finder nella finestra del Terminale e premere Invio.

## 2. Come raccogliere i dati

Per scaricare da Eurostat gli indicatori previsti dal programma, a partire dal 2015:

```bash
python3 mira_pipeline.py eurostat --from-year 2015
```

È necessaria una connessione a Internet. Al termine viene mostrato un **ID esecuzione**: è il nome identificativo di quella specifica raccolta di dati. **Conservarlo**, perché servirà nei passaggi successivi.

Esempio di ID (inventato): `20261008T120000Z_ab12cd34`.

Il programma può anche **importare file CSV territoriali** provenienti da fonti ufficiali già scaricate: è una funzione separata, chiamata `importa`, che richiede il file e le informazioni sulla fonte. Non scarica automaticamente i CSV ISTAT.

## 3. Come ottenere tabelle e note

Sostituire `ID_ESECUZIONE` con l'ID comparso al termine della raccolta:

```bash
python3 mira_pipeline.py report ID_ESECUZIONE
```

Il programma produce una tabella con gli andamenti annuali, una tabella con gli anni in colonna e un testo con note sulle fonti e sulle precauzioni da adottare nell'interpretazione.

## 4. Come ottenere anche i grafici

Per preparare **tabelle, note e figure**, utilizzare il secondo programma:

```bash
python3 mira_pipeline_grafici.py report ID_ESECUZIONE
```

Questo comando crea, oltre alle tabelle, fino a **12 grafici**, ciascuno in due formati:

- **PNG:** immagine adatta a documenti e presentazioni.
- **PDF:** figura di qualità adatta anche all'impaginazione di pubblicazioni.

I grafici mostrano l'evoluzione dei valori negli anni, con uno stile essenziale simile ai grafici prodotti in R con `ggplot2`. Sono creati solo quando una serie comprende **almeno due anni**. Ogni grafico riguarda una serie omogenea: il programma non somma automaticamente fenomeni, territori o categorie differenti.

Per creare fino a 30 grafici:

```bash
python3 mira_pipeline_grafici.py report ID_ESECUZIONE --max-grafici 30
```

**Importante:** sui dati di una stessa esecuzione, usare preferibilmente **uno solo dei due comandi `report`**, scegliendo quello con o senza grafici. I due programmi scrivono gli stessi nomi per alcune tabelle e per il registro degli output; eseguire successivamente il report base potrebbe sostituire il registro che comprende i grafici.

## 5. Dove si trovano i risultati?

I programmi creano una cartella `data` accanto agli script. All'interno, ogni raccolta ha la propria sottocartella, per esempio:

```text
cartella_MIRA/
├── mira_pipeline.py
├── mira_pipeline_grafici.py
├── README_MIRA.md
└── data/
    └── runs/
        └── ID_ESECUZIONE/
            ├── manifest.json              (registro dell'acquisizione)
            ├── osservazioni.csv           (dati raccolti e organizzati)
            ├── andamento_annuo.csv        (valori e variazioni annuali)
            ├── tabella_serie.csv          (tabella con gli anni in colonna)
            ├── note_metodologiche.md      (informazioni sulle fonti e limiti)
            ├── analisi_manifest.json      (registro dei risultati)
            ├── quality.json               (controlli sui dati)
            ├── raw/                       (file originali acquisiti)
            └── grafici/                   (solo con la versione grafica)
                ├── serie_01.png
                ├── serie_01.pdf
                └── ...
```

Il numero di figure dipende dai dati disponibili: se non ci sono serie con almeno due anni, la cartella dei grafici potrebbe non essere creata.

## 6. Come controllare che i file non siano cambiati

Dopo aver raccolto i dati e prodotto il report, eseguire:

```bash
python3 mira_pipeline_grafici.py verifica ID_ESECUZIONE
```

Il controllo confronta le **impronte digitali** dei file (hash SHA-256) con quelle registrate. Serve a individuare modifiche successive ai file: **non garantisce da solo che le statistiche siano corrette**. Il comando `verifica` è disponibile anche in `mira_pipeline.py`.

## Altri comandi utili

Per creare o consultare l'elenco iniziale delle fonti informative:

```bash
python3 mira_pipeline.py fonti
```

Per leggere l'elenco completo delle operazioni disponibili:

```bash
python3 mira_pipeline.py --help
python3 mira_pipeline_grafici.py --help
```

## Cosa bisogna sapere prima di usare i risultati?

I dati provengono da fonti differenti e **non sempre sono direttamente confrontabili**. In particolare, uno *stock* (quante persone risultano presenti a una data) non equivale a un *flusso* (quanti ingressi si verificano in un periodo). Prima di inserire dati o figure in un rapporto, occorre verificare la fonte originale, la definizione dell'indicatore, l'anno, l'unità territoriale e gli eventuali avvisi presenti nelle note metodologiche.