"""Test offline della pipeline MIRA: ``python -m unittest -v``."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import mira_pipeline as mira

# Dataset JSON-stat minimo: 2 paesi x 2 anni, un valore mancante, un flag.
JSONSTAT = {
    "class": "dataset", "label": "Test", "updated": "2026-01-01",
    "id": ["geo", "time"], "size": [2, 2],
    "dimension": {
        "geo": {"category": {"index": {"IT": 0, "EU27_2020": 1},
                             "label": {"IT": "Italy", "EU27_2020": "European Union"}}},
        "time": {"category": {"index": {"2022": 0, "2023": 1}}},
    },
    "value": {"0": 100, "1": 110, "3": 5000},
    "status": {"1": "p"},
}

CSV_PROVINCE = (
    "anno;codice_provincia;provincia;indicatore;valore;unita;fonte_nota\n"
    "2022;001;Torino;Stranieri residenti;1.000,5;persone;a\n"
    "2023;001;Torino;Stranieri residenti;1.100;persone;a\n"
)


def quiet(func, *args):
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return func(*args)


class JsonStatTests(unittest.TestCase):
    def test_normalize(self):
        rows = mira.normalize_jsonstat(JSONSTAT, "tps00157", "https://x", "t")
        self.assertEqual(len(rows), 3)  # il valore None è scartato
        by_key = {(r["codice_territorio"], r["anno"]): r for r in rows}
        self.assertEqual(by_key[("IT", "2023")]["flag"], "p")
        self.assertEqual(by_key[("EU27_2020", "2023")]["livello_territoriale"], "aggregato")
        self.assertEqual(by_key[("IT", "2022")]["tipo_misura"], "stock")
        self.assertEqual(by_key[("IT", "2022")]["territorio"], "Italy")

    def test_invalid(self):
        with self.assertRaises(mira.MiraError):
            mira.normalize_jsonstat({"class": "collection"}, "x", "u", "t")

    def test_position_out_of_range(self):
        bad = {**JSONSTAT, "value": {"9": 1}}
        with self.assertRaises(mira.MiraError):
            mira.normalize_jsonstat(bad, "x", "u", "t")


class ValidationTests(unittest.TestCase):
    def row(self, **kw):
        base = {f: "" for f in mira.FIELDS}
        base.update(fonte="F", dataset="d", anno="2023", valore="1", flag="")
        base.update(kw)
        return base

    def test_duplicates_and_bad_values(self):
        quality = mira.validate_rows([self.row(), self.row(), self.row(valore="abc", anno="2024")])
        self.assertEqual(quality["n_errori"], 2)

    def test_negative_is_warning(self):
        quality = mira.validate_rows([self.row(valore="-3")])
        self.assertEqual((quality["n_errori"], quality["n_valori_negativi"]), (0, 1))


class NumberTests(unittest.TestCase):
    def test_italian_format(self):
        self.assertEqual(mira.normalize_number("1.234,5", ","), "1234.5")
        self.assertEqual(mira.normalize_number("1,234.5", "."), "1234.5")
        with self.assertRaises(mira.MiraError):
            mira.normalize_number("n.d.", ",")


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "data"
        self.addCleanup(self.tmp.cleanup)

    def import_run(self, content=CSV_PROVINCE, *extra):
        source = Path(self.tmp.name) / "fonte.csv"
        source.write_text(content, encoding="utf-8")
        code = quiet(mira.main, [
            "--output", str(self.out), "importa", str(source), "--tipo-misura", "stock",
            "--source-name", "ISTAT", "--source-url", "https://demo.istat.it",
            "--dataset", "test", "--decimal", ",", *extra])
        runs = list((self.out / "runs").iterdir())
        return code, runs[0]

    def test_import_verify_report(self):
        code, folder = self.import_run()
        self.assertEqual(code, 0)
        self.assertEqual(quiet(mira.main, ["--output", str(self.out), "verifica", folder.name]), 0)
        self.assertEqual(quiet(mira.main, ["--output", str(self.out), "report", folder.name]), 0)
        self.assertEqual(quiet(mira.main, ["--output", str(self.out), "verifica", folder.name]), 0)

        changes = (folder / "andamento_annuo.csv").read_text(encoding="utf-8")
        self.assertIn("100", changes)  # 1000.5 -> 1100: +99.5 (+9.95%)
        self.assertIn("99.5", changes)
        self.assertIn("9.95", changes)
        wide = (folder / "tabella_serie.csv").read_text(encoding="utf-8").splitlines()
        self.assertTrue(wide[0].endswith("2022,2023"))
        self.assertIn("stock", (folder / "note_metodologiche.md").read_text(encoding="utf-8"))
        # la colonna extra non va persa
        self.assertIn("fonte_nota", (folder / "osservazioni.csv").read_text(encoding="utf-8"))

    def test_tampering_detected(self):
        _, folder = self.import_run()
        quiet(mira.main, ["--output", str(self.out), "report", folder.name])
        (folder / "andamento_annuo.csv").write_text("manomesso", encoding="utf-8")
        self.assertEqual(quiet(mira.main, ["--output", str(self.out), "verifica", folder.name]), 1)

    def test_bad_code_recorded_in_manifest(self):
        bad = CSV_PROVINCE.replace(";001;", ";1;", 1)
        code, folder = self.import_run(bad)
        self.assertEqual(code, 1)
        manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["stato"], "errore")

    def test_eurostat_mocked(self):
        raw = json.dumps(JSONSTAT).encode()
        with mock.patch.object(mira, "http_get", return_value=raw):
            code = quiet(mira.main, ["--output", str(self.out), "eurostat",
                                     "--geo", "IT", "EU27_2020", "--dataset", "tps00157"])
        self.assertEqual(code, 0)
        folder = next((self.out / "runs").iterdir())
        self.assertEqual(quiet(mira.main, ["--output", str(self.out), "report", folder.name]), 0)
        note = (folder / "note_metodologiche.md").read_text(encoding="utf-8")
        self.assertIn("provvisorio", note)

    def test_catalog(self):
        self.assertEqual(quiet(mira.main, ["--output", str(self.out), "fonti"]), 0)
        self.assertTrue((self.out / "catalogo_fonti.csv").exists())


if __name__ == "__main__":
    unittest.main()
