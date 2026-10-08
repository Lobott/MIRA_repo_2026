#!/usr/bin/env python3
"""MIRA 2026/2027 – pipeline riproducibile per fonti e dati sull'immigrazione.

Strumento di supporto alle attività di ricerca del progetto MIRA:

  * ricognizione delle fonti (catalogo, verifica dei collegamenti, bibliografia);
  * acquisizione e normalizzazione di dati aggregati (Eurostat via API;
    CSV ufficiali ISTAT o di altri enti a livello regionale/provinciale/comunale);
  * controlli di qualità, tracciabilità (manifest + SHA-256) e verifica;
  * output pronti per la curatela del volume dell'Osservatorio sulle persone
    straniere: serie annuali, tabelle larghe (anni in colonna) e note
    metodologiche con flag e citazioni delle fonti.

Principi: nessuna credenziale, nessun dato personale (solo dati aggregati),
sola libreria standard, Python >= 3.10.

Comandi (``python mira_pipeline.py --help``):
    fonti             catalogo delle fonti, verifica URL, bibliografia
    eurostat          scarica e normalizza indicatori Eurostat
    importa           importa un CSV ufficiale già scaricato
    verifica          controlla l'integrità di un'esecuzione
    report            serie annuali, tabella larga e note metodologiche
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

__version__ = "2.0.0"

log = logging.getLogger("mira")

# --------------------------------------------------------------------------- #
# Costanti
# --------------------------------------------------------------------------- #
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = ROOT / "data"

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
USER_AGENT = f"MIRA-research/{__version__} (aggregate-open-data; no-personal-data)"
HTTP_TIMEOUT_S = 40
MAX_RESPONSE_BYTES = 30_000_000
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
YEAR_RANGE = (1900, 2100)
MAX_ERRORS_LISTED = 50

# Stock = consistenze a una data (es. residenti al 1° gennaio);
# flusso = eventi in un periodo (es. immigrazioni nell'anno).
# Confrontarli o sommarli è un errore metodologico classico: il campo
# ``tipo_misura`` lo rende esplicito in ogni riga.
MEASURE_TYPES = ("stock", "flusso")


@dataclass(frozen=True)
class DatasetSpec:
    code: str
    description: str
    measure: str  # uno di MEASURE_TYPES


EUROSTAT_DATASETS: dict[str, DatasetSpec] = {
    spec.code: spec
    for spec in (
        DatasetSpec(
            "tps00157",
            "Persone residenti prive della cittadinanza del Paese di residenza "
            "al 1° gennaio",
            "stock",
        ),
        DatasetSpec(
            "tps00176",
            "Immigrazioni annuali (totale, tutte le cittadinanze)",
            "flusso",
        ),
    )
}

# Livelli territoriali importabili da CSV: pattern del codice ISTAT.
TERRITORIAL_LEVELS: dict[str, str] = {
    "regione": r"\d{2}",
    "provincia": r"\d{3}",
    "comune": r"\d{6}",
}
IMPORT_REQUIRED = ("anno", "indicatore", "valore", "unita")
IMPORT_OPTIONAL = ("cittadinanza", "sesso", "eta", "flag")

# Legenda dei flag Eurostat (un carattere = un flag; possono essere combinati).
EUROSTAT_FLAGS = {
    "b": "rottura di serie",
    "c": "confidenziale",
    "d": "definizione diversa",
    "e": "stimato",
    "f": "previsione",
    "n": "non significativo",
    "p": "provvisorio",
    "r": "rivisto",
    "s": "stima Eurostat",
    "u": "bassa affidabilità",
    "z": "non applicabile",
}

FIELDS = [
    "fonte", "dataset", "indicatore", "tipo_misura", "anno",
    "livello_territoriale", "codice_territorio", "territorio",
    "cittadinanza", "sesso", "eta", "valore", "unita", "flag",
    "altre_dimensioni", "url_origine", "data_estrazione_utc",
]

# Dimensioni che identificano una serie storica (tutto tranne anno e valore).
SERIES_FIELDS = [
    "fonte", "dataset", "indicatore", "tipo_misura", "livello_territoriale",
    "codice_territorio", "territorio", "cittadinanza", "sesso", "eta",
    "unita", "altre_dimensioni",
]

# Dimensioni JSON-stat già mappate su colonne dedicate.
JSONSTAT_MAPPED = {"time", "geo", "citizen", "sex", "age", "unit"}

# --------------------------------------------------------------------------- #
# Catalogo delle fonti (ricognizione)
# --------------------------------------------------------------------------- #
CATALOG_FIELDS = [
    "id", "ente", "nome", "url", "ambito", "tipo_misura",
    "livello_territoriale", "periodicita", "formato", "licenza", "note",
    "stato_verifica", "data_verifica", "esito_http",
]

# Punto di partenza: indirizzi di primo livello, da verificare e completare
# durante la ricognizione (periodicità, formato, licenza, ultimo anno
# disponibile). Il catalogo è un CSV modificabile a mano.
_SEED = [
    ("istat_residenti", "ISTAT", "Popolazione residente e stranieri residenti",
     "https://demo.istat.it", "demografia", "stock", "comune/provincia/regione"),
    ("istat_dati", "ISTAT", "IstatData – banca dati statistica",
     "https://esploradati.istat.it", "multi-tema", "", ""),
    ("interno_permessi", "Ministero dell'Interno",
     "Permessi di soggiorno e dati su sbarchi/asilo",
     "https://www.interno.gov.it", "soggiorno, asilo", "", ""),
    ("istruzione_alunni", "Ministero dell'Istruzione e del Merito",
     "Alunni con cittadinanza non italiana (Portale unico dei dati della scuola)",
     "https://dati.istruzione.it", "scuola", "stock", "scuola/provincia/regione"),
    ("lavoro_comunita", "Ministero del Lavoro e delle Politiche Sociali",
     "Rapporti annuali sul mercato del lavoro dei migranti",
     "https://www.lavoro.gov.it", "lavoro", "", ""),
    ("inps_osservatori", "INPS", "Osservatori statistici",
     "https://www.inps.it", "lavoro, previdenza", "", ""),
    ("eurostat_migr", "Eurostat", "Migration and asylum",
     "https://ec.europa.eu/eurostat/web/migration-asylum", "migrazioni, asilo",
     "", "paese"),
    ("euaa", "EUAA", "Agenzia dell'UE per l'asilo – dati e analisi",
     "https://euaa.europa.eu", "asilo", "", "paese"),
    ("unhcr", "UNHCR", "Refugee statistics",
     "https://www.unhcr.org", "rifugiati, asilo", "", "paese"),
    ("oecd", "OECD", "International Migration Outlook e banche dati",
     "https://www.oecd.org", "migrazioni, lavoro", "", "paese"),
    ("iom_portal", "IOM", "Migration Data Portal",
     "https://www.migrationdataportal.org", "multi-tema", "", "paese"),
    ("ismu", "Fondazione ISMU", "Rapporto annuale sulle migrazioni",
     "https://www.ismu.org", "multi-tema", "", ""),
    ("idos", "Centro Studi e Ricerche IDOS", "Dossier Statistico Immigrazione",
     "https://www.dossierimmigrazione.it", "multi-tema", "", ""),
]


class MiraError(Exception):
    """Errore atteso (dati, rete, configurazione) con messaggio per l'utente."""


# --------------------------------------------------------------------------- #
# Utilità generali
# --------------------------------------------------------------------------- #
def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}_{uuid.uuid4().hex[:8]}"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_replace(path: Path, writer: Callable[[Path], None]) -> None:
    """Scrive su un file temporaneo e lo rinomina: mai file a metà."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    writer(tmp)
    tmp.replace(path)


def write_bytes(path: Path, data: bytes) -> None:
    _atomic_replace(path, lambda tmp: tmp.write_bytes(data))


def write_text(path: Path, text: str) -> None:
    write_bytes(path, text.encode("utf-8"))


def write_json(path: Path, value: Any) -> None:
    text = json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True)
    write_text(path, text + "\n")


def write_csv(path: Path, rows: Iterable[dict], fields: Sequence[str] = FIELDS) -> None:
    def _write(tmp: Path) -> None:
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    _atomic_replace(path, _write)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def registered_file(path: Path, root: Path) -> dict:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def format_decimal(value: Decimal) -> str:
    """Decimal → stringa senza notazione scientifica."""
    return format(value, "f")


def normalize_number(text: str, decimal: str = ".") -> str:
    """Normalizza un numero testuale in forma canonica (punto decimale).

    Con ``decimal=","`` il punto è trattato come separatore delle migliaia
    (formato italiano: ``1.234,5`` → ``1234.5``).
    """
    cleaned = text.strip().replace("\u00a0", "").replace(" ", "")
    if decimal == ",":
        cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
        cleaned = cleaned.replace(",", "")
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise MiraError(f"valore non numerico: {text!r}") from exc
    if not value.is_finite():
        raise MiraError(f"valore non finito: {text!r}")
    return format_decimal(value)


def citation(entity: str, title: str, url: str, accessed: str) -> str:
    """Riferimento bibliografico sintetico per note e didascalie."""
    return f"{entity}, {title}, {url} (ultimo accesso: {accessed[:10]})."


# --------------------------------------------------------------------------- #
# Rete
# --------------------------------------------------------------------------- #
def http_get(url: str, attempts: int = 3, max_bytes: int = MAX_RESPONSE_BYTES) -> bytes:
    """GET con timeout, limite di dimensione e retry esponenziale."""
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    for attempt in range(attempts):
        last = attempt == attempts - 1
        try:
            with urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
                raw = response.read(max_bytes + 1)
        except HTTPError as exc:
            if exc.code not in RETRY_STATUS or last:
                raise MiraError(f"HTTP {exc.code} per {url}") from exc
        except (URLError, TimeoutError) as exc:
            if last:
                raise MiraError(f"Errore di rete per {url}: {exc}") from exc
        else:
            if len(raw) > max_bytes:
                raise MiraError(
                    f"Risposta oltre {max_bytes // 1_000_000} MB: restringere i filtri"
                )
            return raw
        time.sleep(2 ** attempt)
    raise MiraError("Numero di tentativi esaurito")  # pragma: no cover


def get_json(url: str) -> tuple[dict, bytes]:
    """Restituisce JSON interpretato e byte originali (da archiviare)."""
    raw = http_get(url)
    try:
        return json.loads(raw), raw
    except json.JSONDecodeError as exc:
        raise MiraError(f"La risposta non è JSON valido: {url}") from exc


# --------------------------------------------------------------------------- #
# JSON-stat 2.0 → formato long
# --------------------------------------------------------------------------- #
def code_sequence(dimension: dict, expected_size: int) -> list[str]:
    """Codici delle categorie di una dimensione, in ordine di posizione."""
    index = dimension["category"]["index"]
    if isinstance(index, dict):
        sequence: list[str | None] = [None] * expected_size
        for code, pos in index.items():
            if not isinstance(pos, int) or not 0 <= pos < expected_size or sequence[pos] is not None:
                raise MiraError("Indice delle dimensioni non coerente")
            sequence[pos] = code
    elif isinstance(index, list):
        sequence = list(index)
    else:
        raise MiraError("Formato indice JSON-stat sconosciuto")
    if len(sequence) != expected_size or any(code is None for code in sequence):
        raise MiraError("Dimensione JSON-stat incoerente")
    return sequence  # type: ignore[return-value]


def _unravel(position: int, sizes: Sequence[int]) -> list[int]:
    """Posizione lineare → indice per ciascuna dimensione (ordine row-major)."""
    indices = []
    for size in reversed(sizes):
        position, index = divmod(position, size)
        indices.append(index)
    return indices[::-1]


def _observations(raw_values: Any) -> Iterator[tuple[int, Any]]:
    if isinstance(raw_values, list):
        return enumerate(raw_values)
    if isinstance(raw_values, dict):
        return ((int(key), value) for key, value in raw_values.items())
    raise MiraError("Valori JSON-stat in formato inatteso")


def _flag_at(statuses: Any, position: int) -> str:
    if isinstance(statuses, list):
        return (statuses[position] if position < len(statuses) else "") or ""
    return (statuses or {}).get(str(position), "") or ""


def territorial_level(code: str) -> str:
    """Classifica un codice geo Eurostat: paese, aggregato (UE…) o NUTS."""
    if code.startswith(("EU", "EA")) and len(code) > 2:
        return "aggregato"
    return "paese" if len(code) == 2 else "nuts"


def normalize_jsonstat(data: dict, dataset: str, url: str, acquired_at: str) -> list[dict]:
    """Da JSON-stat 2.0 a righe long con le colonne di ``FIELDS``."""
    if data.get("class") != "dataset":
        raise MiraError("La risposta non contiene un dataset JSON-stat valido")
    try:
        ids, sizes = data["id"], data["size"]
        dimensions, raw_values = data["dimension"], data["value"]
    except KeyError as exc:
        raise MiraError(f"Campo JSON-stat mancante: {exc}") from exc
    if len(ids) != len(sizes):
        raise MiraError("Lunghezza delle dimensioni non coerente")

    codes = [code_sequence(dimensions[name], sizes[i]) for i, name in enumerate(ids)]
    labels = [dimensions[name]["category"].get("label", {}) for name in ids]
    total = 1
    for size in sizes:
        total *= size

    spec = EUROSTAT_DATASETS.get(dataset)
    indicator = spec.description if spec else data.get("label", dataset)
    measure = spec.measure if spec else "non_specificato"
    statuses = data.get("status")

    rows = []
    for position, value in _observations(raw_values):
        if value is None:
            continue
        if not 0 <= position < total:
            raise MiraError("Posizione di una osservazione oltre il dataset")
        indices = _unravel(position, sizes)
        attrs = {name: codes[i][indices[i]] for i, name in enumerate(ids)}
        geo = attrs.get("geo", "")
        extra = {k: v for k, v in attrs.items() if k not in JSONSTAT_MAPPED}
        geo_label = labels[ids.index("geo")].get(geo, geo) if "geo" in ids else ""
        rows.append({
            "fonte": "Eurostat", "dataset": dataset, "indicatore": indicator,
            "tipo_misura": measure, "anno": attrs.get("time", ""),
            "livello_territoriale": territorial_level(geo),
            "codice_territorio": geo, "territorio": geo_label,
            "cittadinanza": attrs.get("citizen", ""), "sesso": attrs.get("sex", ""),
            "eta": attrs.get("age", ""), "valore": str(value),
            "unita": attrs.get("unit", ""), "flag": _flag_at(statuses, position),
            "altre_dimensioni": json.dumps(extra, sort_keys=True),
            "url_origine": url, "data_estrazione_utc": acquired_at,
        })
    return rows


# --------------------------------------------------------------------------- #
# Controlli di qualità
# --------------------------------------------------------------------------- #
def _check_observation(row: dict) -> tuple[Decimal | None, str | None]:
    """(valore, problema): problema è None se la riga è valida."""
    try:
        year = int(row["anno"])
        if not YEAR_RANGE[0] <= year <= YEAR_RANGE[1]:
            return None, "anno fuori intervallo"
        value = Decimal(row["valore"])
        if not value.is_finite():
            return None, "valore non finito"
    except (KeyError, ValueError, InvalidOperation) as exc:
        return None, f"anno o valore non interpretabile ({exc.__class__.__name__})"
    return value, None


def validate_rows(rows: Sequence[dict]) -> dict:
    """Errori (bloccanti) e avvisi (da guardare prima di pubblicare)."""
    seen: set[tuple] = set()
    errors: list[str] = []
    n_errors = n_negative = n_flag = 0
    flags: Counter[str] = Counter()
    flag_examples: list[str] = []
    key_fields = ("fonte", "dataset", "anno", "livello_territoriale", "codice_territorio",
                  "indicatore", "tipo_misura", "cittadinanza", "sesso", "eta", "unita",
                  "altre_dimensioni")

    def add_error(message: str) -> None:
        nonlocal n_errors
        n_errors += 1
        if len(errors) < MAX_ERRORS_LISTED:
            errors.append(message)

    for line, row in enumerate(rows, 1):
        value, problem = _check_observation(row)
        if problem:
            add_error(f"Osservazione {line}: {problem}")
        elif value is not None and value < 0:
            n_negative += 1  # ammesso (es. saldi) ma segnalato
        key = tuple(row.get(field, "") for field in key_fields)
        if key in seen:
            add_error(f"Osservazione {line}: combinazione dimensionale duplicata")
        seen.add(key)
        if row.get("flag"):
            n_flag += 1
            flags[row["flag"]] += 1
            if len(flag_examples) < 10:
                flag_examples.append(f"Osservazione {line}: flag {row['flag']}")
    return {
        "righe": len(rows), "n_errori": n_errors, "errori": errors,
        "n_valori_negativi": n_negative, "n_flag": n_flag,
        "flag_distinti": dict(sorted(flags.items())), "esempi_flag": flag_examples,
    }


def require_quality(quality: dict) -> None:
    if quality["n_errori"]:
        shown = "; ".join(quality["errori"][:3])
        raise MiraError(f"Controlli di qualità falliti ({quality['n_errori']} errori): {shown}")


# --------------------------------------------------------------------------- #
# Esecuzione tracciata
# --------------------------------------------------------------------------- #
class Run:
    """Una esecuzione: cartella dedicata + manifest con hash dei file."""

    def __init__(self, output: Path, activity: str) -> None:
        self.root = output.resolve()
        self.id = new_run_id()
        self.folder = self.root / "runs" / self.id
        self.extracted = now_utc()
        self.manifest: dict[str, Any] = {
            "run_id": self.id, "versione_pipeline": __version__,
            "attivita": activity, "estratto_utc": self.extracted,
            "versione_script_sha256": sha256_file(Path(__file__)),
            "fonti": [], "file": [], "stato": "in_corso",
            "nota": "La data di estrazione non prova la data di sviluppo del software.",
        }

    @property
    def manifest_path(self) -> Path:
        return self.folder / "manifest.json"

    def register(self, path: Path) -> None:
        self.manifest["file"].append(registered_file(path, self.root))

    def save(self) -> None:
        write_json(self.manifest_path, self.manifest)
        print(f"ID esecuzione: {self.id}\nManifest: {self.manifest_path}")


def run_activity(args: argparse.Namespace, activity: str,
                 body: Callable[[Run, argparse.Namespace], None]) -> int:
    """Esegue ``body`` e scrive sempre il manifest, anche in caso di errore."""
    run = Run(args.output, activity)
    try:
        body(run, args)
        run.manifest["stato"] = "completato"
    except Exception as exc:  # noqa: BLE001 - l'errore va registrato nel manifest
        run.manifest.update(stato="errore", errore=str(exc))
        log.error("%s", exc)
    finally:
        run.save()
    return 0 if run.manifest["stato"] == "completato" else 1


def _finalize_dataset(run: Run, rows: list[dict]) -> None:
    """Qualità → CSV → manifest: comune a eurostat e importa."""
    if not rows:
        raise MiraError("Nessuna osservazione: verificare filtri, file e fonte")
    quality = validate_rows(rows)
    quality_path = run.folder / "quality.json"
    write_json(quality_path, quality)
    run.register(quality_path)
    require_quality(quality)
    data_path = run.folder / "osservazioni.csv"
    write_csv(data_path, rows)
    run.register(data_path)
    run.manifest["osservazioni"] = len(rows)


# --------------------------------------------------------------------------- #
# Comando: eurostat
# --------------------------------------------------------------------------- #
def _eurostat_body(run: Run, args: argparse.Namespace) -> None:
    codes = args.dataset or list(EUROSTAT_DATASETS)
    rows: list[dict] = []
    for code in codes:
        spec = EUROSTAT_DATASETS[code]
        params = [("geo", geo) for geo in args.geo]
        params += [("sinceTimePeriod", str(args.from_year)), ("lang", "EN"), ("format", "JSON")]
        if args.to_year:
            params.append(("untilTimePeriod", str(args.to_year)))
        url = f"{EUROSTAT_API}{code}?{urlencode(params)}"
        result, original = get_json(url)
        raw_path = run.folder / "raw" / f"{code}.json"
        write_bytes(raw_path, original)
        run.register(raw_path)
        normalized = normalize_jsonstat(result, code, url, run.extracted)
        rows.extend(normalized)
        run.manifest["fonti"].append({
            "dataset": code, "descrizione": spec.description, "tipo_misura": spec.measure,
            "ente": "Eurostat", "url": url,
            "ultimo_aggiornamento_fonte": result.get("updated"),
            "osservazioni": len(normalized),
        })
    _finalize_dataset(run, rows)


def eurostat_command(args: argparse.Namespace) -> int:
    return run_activity(args, "eurostat", _eurostat_body)


# --------------------------------------------------------------------------- #
# Comando: importa (CSV ufficiali a livello regionale/provinciale/comunale)
# --------------------------------------------------------------------------- #
def _cell(item: dict, key: str) -> str:
    return (item.get(key) or "").strip()


def parse_territorial_csv(content: bytes, *, level: str, dataset: str, source: str,
                          url: str, measure: str, acquired_at: str,
                          delimiter: str = ";", decimal: str = ".",
                          encoding: str = "utf-8-sig") -> list[dict]:
    """CSV ufficiale → righe nel formato standard.

    Colonne obbligatorie: anno, indicatore, valore, unita, ``codice_<livello>``
    e ``<livello>``. Facoltative: cittadinanza, sesso, eta, flag. Eventuali
    altre colonne finiscono in ``altre_dimensioni`` (nulla va perso) e, come
    ogni dimensione, distinguono le serie: valori diversi = serie diverse.
    """
    code_col, name_col = f"codice_{level}", level
    pattern = re.compile(TERRITORIAL_LEVELS[level])
    reader = csv.DictReader(io.StringIO(content.decode(encoding)), delimiter=delimiter)
    required = {*IMPORT_REQUIRED, code_col, name_col}
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise MiraError("Colonne richieste assenti: " + ", ".join(sorted(missing)))
    known = required | set(IMPORT_OPTIONAL)
    rows = []
    for number, item in enumerate(reader, 2):  # riga 1 = intestazione
        code = _cell(item, code_col)
        if not pattern.fullmatch(code):
            raise MiraError(
                f"Riga {number}: {code_col} non valido ({code!r}): "
                f"atteso il codice ISTAT per il livello «{level}»"
            )
        for column in ("indicatore", name_col, "unita"):
            if not _cell(item, column):
                raise MiraError(f"Riga {number}: colonna «{column}» non valorizzata")
        try:
            value = normalize_number(_cell(item, "valore"), decimal)
        except MiraError as exc:
            raise MiraError(f"Riga {number}: {exc}") from exc
        extra = {k: _cell(item, k) for k in item if k and k not in known}
        rows.append({
            "fonte": source, "dataset": dataset, "indicatore": _cell(item, "indicatore"),
            "tipo_misura": measure, "anno": _cell(item, "anno"),
            "livello_territoriale": level, "codice_territorio": code,
            "territorio": _cell(item, name_col),
            "cittadinanza": _cell(item, "cittadinanza"), "sesso": _cell(item, "sesso"),
            "eta": _cell(item, "eta"), "valore": value, "unita": _cell(item, "unita"),
            "flag": _cell(item, "flag"),
            "altre_dimensioni": json.dumps(extra, sort_keys=True, ensure_ascii=False),
            "url_origine": url, "data_estrazione_utc": acquired_at,
        })
    return rows


def _import_body(run: Run, args: argparse.Namespace) -> None:
    parsed = urlparse(args.source_url)
    if parsed.scheme != "https" or not parsed.netloc:
        raise MiraError("Indicare il collegamento HTTPS ufficiale al dataset pubblicato")
    original = args.file.read_bytes()
    raw_path = run.folder / "raw" / "fonte_originale.csv"
    write_bytes(raw_path, original)
    run.register(raw_path)
    rows = parse_territorial_csv(
        original, level=args.livello, dataset=args.dataset, source=args.source_name,
        url=args.source_url, measure=args.tipo_misura, acquired_at=run.extracted,
        delimiter=args.delimiter, decimal=args.decimal, encoding=args.encoding,
    )
    run.manifest["fonti"].append({
        "dataset": args.dataset, "descrizione": args.dataset, "ente": args.source_name,
        "tipo_misura": args.tipo_misura, "url": args.source_url,
        "file_originale": args.file.name, "osservazioni": len(rows),
    })
    run.manifest["nota"] = (
        "Importazione di un file indicato dall'operatore: la corrispondenza con la "
        "fonte ufficiale va controllata prima della pubblicazione."
    )
    _finalize_dataset(run, rows)


def import_command(args: argparse.Namespace) -> int:
    return run_activity(args, f"importazione_{args.livello}", _import_body)


# --------------------------------------------------------------------------- #
# Verifica di integrità
# --------------------------------------------------------------------------- #
def check_files(root: Path, items: Iterable[dict]) -> tuple[int, list[str]]:
    """Controlla esistenza, confinamento in ``root`` e hash. → (ok, errori)."""
    ok, failures = 0, []
    for item in items:
        path = (root / item["path"]).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            failures.append(f"File non disponibile o esterno: {item['path']}")
        elif sha256_file(path) != item["sha256"]:
            failures.append(f"Hash non corrispondente: {item['path']}")
        else:
            ok += 1
    return ok, failures


def verify_command(args: argparse.Namespace) -> int:
    root = args.output.resolve()
    folder = root / "runs" / args.run
    manifest_path = folder / "manifest.json"
    manifest = read_json(manifest_path)
    verified, failures = check_files(root, manifest["file"])

    analysis_path = folder / "analisi_manifest.json"
    if analysis_path.exists():
        analysis = read_json(analysis_path)
        ok, problems = check_files(root, analysis["file"])
        verified += ok
        failures += [f"Analisi: {p}" for p in problems]
        if analysis.get("input_manifest_sha256") != sha256_file(manifest_path):
            failures.append("Analisi: manifest modificato dopo la generazione del report")

    print(json.dumps({
        "run_id": args.run, "stato_manifest": manifest["stato"],
        "file_verificati": verified, "errori": failures,
    }, indent=2, ensure_ascii=False))
    return 1 if failures or manifest["stato"] != "completato" else 0


# --------------------------------------------------------------------------- #
# Report: serie annuali, tabella larga, note metodologiche
# --------------------------------------------------------------------------- #
def load_series(rows: Iterable[dict]) -> dict[tuple, dict[int, dict]]:
    """Raggruppa per serie: livelli geografici, definizioni e unità restano separati."""
    series: dict[tuple, dict[int, dict]] = defaultdict(dict)
    for row in rows:
        key = tuple(row[field] for field in SERIES_FIELDS)
        series[key][int(row["anno"])] = row
    return series


def yearly_changes(series: dict[tuple, dict[int, dict]]) -> list[dict]:
    """Variazioni annue descrittive (solo tra anni consecutivi)."""
    result = []
    for key, years in series.items():
        meta = dict(zip(SERIES_FIELDS, key))
        meta.pop("altre_dimensioni")
        for year in sorted(years):
            current, previous = years[year], years.get(year - 1)
            delta = delta_pct = ""
            notes = []
            if previous:
                now, before = Decimal(current["valore"]), Decimal(previous["valore"])
                change = now - before
                delta = format_decimal(change)
                if before != 0:
                    delta_pct = format_decimal((change * 100 / before).quantize(Decimal("0.01")))
            elif any(y < year for y in years):
                notes.append("anno precedente mancante: variazione non calcolata")
            if current["flag"] or (previous and previous["flag"]):
                notes.append("esaminare flag/rotture di serie prima di confrontare")
            result.append({
                **meta, "anno": year, "valore": current["valore"],
                "variazione_assoluta_annua": delta,
                "variazione_percentuale_annua": delta_pct,
                "flag": current["flag"], "nota": "; ".join(notes),
            })
    result.sort(key=lambda r: (r["dataset"], r["codice_territorio"], r["cittadinanza"],
                               r["sesso"], r["eta"], r["anno"]))
    return result


def wide_table(series: dict[tuple, dict[int, dict]]) -> tuple[list[str], list[dict]]:
    """Una riga per serie, un anno per colonna: formato da tabella editoriale."""
    years = sorted({year for ys in series.values() for year in ys})
    visible = [f for f in SERIES_FIELDS if f != "altre_dimensioni"]
    fields = visible + ["altre_dimensioni"] + [str(y) for y in years]
    rows = []
    for key in sorted(series):
        row = dict(zip(SERIES_FIELDS, key))
        row.update({str(y): series[key][y]["valore"] for y in series[key]})
        rows.append(row)
    return fields, rows


def methodology_note(manifest: dict, rows: Sequence[dict]) -> str:
    """Note metodologiche in Markdown, riutilizzabili per la curatela."""
    lines = [
        f"# Note metodologiche – esecuzione {manifest['run_id']}", "",
        f"- Estrazione: {manifest['estratto_utc']} (UTC)",
        f"- Pipeline MIRA v{manifest.get('versione_pipeline', '?')}; "
        f"script SHA-256 `{manifest['versione_script_sha256'][:12]}…`",
        "- Dati aggregati e pubblici; nessun dato personale trattato.", "",
        "## Fonti", "",
    ]
    for src in manifest["fonti"]:
        entity = src.get("ente", "Fonte")
        title = f"{src['descrizione']} [{src['dataset']}]"
        lines.append(f"- {citation(entity, title, src['url'], manifest['estratto_utc'])} "
                     f"Tipo di misura: **{src.get('tipo_misura', 'n.d.')}**; "
                     f"osservazioni: {src.get('osservazioni', 'n.d.')}.")
        if src.get("ultimo_aggiornamento_fonte"):
            lines.append(f"  Ultimo aggiornamento dichiarato dalla fonte: "
                         f"{src['ultimo_aggiornamento_fonte']}.")

    flags = Counter(row["flag"] for row in rows if row["flag"])
    lines += ["", "## Flag presenti", ""]
    if flags:
        for flag, count in sorted(flags.items()):
            meaning = ", ".join(EUROSTAT_FLAGS.get(c, c) for c in flag)
            lines.append(f"- `{flag}` ({count} osservazioni): {meaning}")
    else:
        lines.append("- Nessun flag.")

    lines += [
        "", "## Avvertenze per l'uso nel volume", "",
        "- **Stock e flussi** non vanno sommati né confrontati direttamente: "
        "vedere la colonna `tipo_misura`.",
        "- Le variazioni sono **descrittive**: non certificano la comparabilità "
        "metodologica tra anni, territori e fonti.",
        "- Prima di confrontare gli anni controllare flag, rotture di serie e "
        "revisioni; definizioni di «straniero» e di «immigrazione» differiscono "
        "tra enti e Paesi.",
        "- Livelli territoriali diversi (paese, regione, provincia, comune) "
        "restano in serie separate.",
        "- Citare la fonte originale e la data di accesso; verificare i valori "
        "chiave sul sito dell'ente prima della pubblicazione.", "",
    ]
    return "\n".join(lines)


def report_command(args: argparse.Namespace) -> int:
    root = args.output.resolve()
    folder = root / "runs" / args.run
    manifest = read_json(folder / "manifest.json")
    if manifest["stato"] != "completato":
        raise MiraError("Report disponibile solo per esecuzioni completate")
    _, failures = check_files(root, manifest["file"])
    if failures:
        raise MiraError("Integrità non verificata: " + "; ".join(failures))

    with (folder / "osservazioni.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    series = load_series(rows)

    changes = yearly_changes(series)
    changes_path = folder / "andamento_annuo.csv"
    write_csv(changes_path, changes, list(changes[0]) if changes else [])

    wide_fields, wide_rows = wide_table(series)
    wide_path = folder / "tabella_serie.csv"
    write_csv(wide_path, wide_rows, wide_fields)

    note_path = folder / "note_metodologiche.md"
    write_text(note_path, methodology_note(manifest, rows))

    outputs = (changes_path, wide_path, note_path)
    write_json(folder / "analisi_manifest.json", {
        "run_id": args.run, "analisi_utc": now_utc(),
        "input_manifest_sha256": sha256_file(folder / "manifest.json"),
        "versione_script_sha256": sha256_file(Path(__file__)),
        "file": [registered_file(p, root) for p in outputs],
        "nota": "Il report deriva dai file della specifica esecuzione e non "
                "certifica comparabilità metodologica.",
    })
    print(f"Serie distinte: {len(series)}; osservazioni: {len(rows)}")
    for path in outputs:
        print(f"  {path}")
    print("Le variazioni sono descrittive. Non confrontare stock e flussi e verificare i flag.")
    return 0


# --------------------------------------------------------------------------- #
# Comando: fonti (ricognizione)
# --------------------------------------------------------------------------- #
def _seed_catalog() -> list[dict]:
    rows = []
    for ident, entity, name, url, scope, measure, level in _SEED:
        row = dict.fromkeys(CATALOG_FIELDS, "")
        row.update(id=ident, ente=entity, nome=name, url=url, ambito=scope,
                   tipo_misura=measure, livello_territoriale=level,
                   stato_verifica="da_verificare",
                   note="Da completare: periodicità, formato, licenza, ultimo anno disponibile")
        rows.append(row)
    return rows


def load_catalog(path: Path) -> list[dict]:
    if not path.exists():
        write_csv(path, _seed_catalog(), CATALOG_FIELDS)
        log.info("Creato catalogo iniziale: %s", path)
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def check_url(url: str) -> str:
    """Esito sintetico di un collegamento (senza scaricarne il contenuto)."""
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
            response.read(1024)
            return f"HTTP {response.status}"
    except HTTPError as exc:
        return f"HTTP {exc.code}"
    except (URLError, TimeoutError) as exc:
        return f"errore: {exc}"


def sources_command(args: argparse.Namespace) -> int:
    path = args.catalogo or args.output.resolve() / "catalogo_fonti.csv"
    catalog = load_catalog(path)

    if args.verifica:
        today = now_utc()[:10]
        for row in catalog:
            row["esito_http"] = check_url(row["url"])
            row["data_verifica"] = today
            if row["esito_http"].startswith("HTTP 2"):
                row["stato_verifica"] = "raggiungibile"
            else:
                row["stato_verifica"] = "da_controllare"
            print(f"{row['id']:<20} {row['esito_http']}")
            time.sleep(1)  # cortesia verso i server
        write_csv(path, catalog, CATALOG_FIELDS)

    if args.bibliografia:
        today = now_utc()
        for row in sorted(catalog, key=lambda r: (r["ente"], r["nome"])):
            print("- " + citation(row["ente"], row["nome"], row["url"],
                                  row["data_verifica"] or today))

    if not (args.verifica or args.bibliografia):
        print(f"Catalogo: {path} ({len(catalog)} fonti)")
        counts = Counter(row["stato_verifica"] for row in catalog)
        print("Stato: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return 0


# --------------------------------------------------------------------------- #
# Interfaccia a riga di comando
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mira_pipeline",
        description="MIRA 2026/2027 – ricognizione fonti e analisi di dati "
                    "aggregati sull'immigrazione",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="cartella degli output (predefinita: ./data)")
    parser.add_argument("-v", "--verbose", action="store_true", help="log dettagliato")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="action", required=True)

    src = sub.add_parser("fonti", help="catalogo delle fonti (ricognizione)")
    src.add_argument("--catalogo", type=Path, help="CSV del catalogo (predefinito: <output>/catalogo_fonti.csv)")
    src.add_argument("--verifica", action="store_true", help="controlla la raggiungibilità degli URL")
    src.add_argument("--bibliografia", action="store_true", help="stampa i riferimenti in Markdown")
    src.set_defaults(handler=sources_command)

    euro = sub.add_parser("eurostat", help="scarica e normalizza indicatori Eurostat")
    euro.add_argument("--geo", nargs="+", default=["IT"], metavar="CODICE",
                      help="codici geo Eurostat, es. IT DE FR EU27_2020 (predefinito: IT)")
    euro.add_argument("--dataset", nargs="+", choices=sorted(EUROSTAT_DATASETS),
                      help="tabelle da scaricare (predefinito: tutte)")
    euro.add_argument("--from-year", type=int, default=2015, help="primo anno (predefinito: 2015)")
    euro.add_argument("--to-year", type=int, help="ultimo anno (predefinito: il più recente)")
    euro.set_defaults(handler=eurostat_command)

    imp = sub.add_parser("importa", help="importa un CSV ufficiale già scaricato")
    imp.add_argument("file", type=Path)
    imp.add_argument("--livello", choices=sorted(TERRITORIAL_LEVELS), default="provincia")
    imp.add_argument("--tipo-misura", choices=MEASURE_TYPES, required=True,
                     help="stock (consistenza a una data) o flusso (eventi nel periodo)")
    imp.add_argument("--source-name", required=True, help="ente che pubblica i dati (es. ISTAT)")
    imp.add_argument("--source-url", required=True, help="pagina HTTPS ufficiale del dataset")
    imp.add_argument("--dataset", required=True, help="identificativo o titolo della tabella")
    imp.add_argument("--delimiter", default=";", help="separatore CSV (predefinito: ;)")
    imp.add_argument("--decimal", choices=(".", ","), default=".",
                     help="separatore decimale; con ',' il punto è delle migliaia")
    imp.add_argument("--encoding", default="utf-8-sig", help="codifica del file (es. latin-1)")
    imp.set_defaults(handler=import_command)

    ver = sub.add_parser("verifica", help="verifica gli hash SHA-256 di una esecuzione")
    ver.add_argument("run", help="ID esecuzione")
    ver.set_defaults(handler=verify_command)

    rep = sub.add_parser("report", help="serie annuali, tabella larga, note metodologiche")
    rep.add_argument("run", help="ID esecuzione")
    rep.set_defaults(handler=report_command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s: %(message)s", stream=sys.stderr)
    return args.handler(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (MiraError, OSError, ValueError) as exc:
        print(f"ERRORE: {exc}", file=sys.stderr)
        sys.exit(1)
