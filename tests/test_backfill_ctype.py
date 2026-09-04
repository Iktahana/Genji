import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "script" / "backfill_ctype.py"
SPEC = importlib.util.spec_from_file_location("backfill_ctype", SCRIPT)
assert SPEC and SPEC.loader
backfill = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(backfill)


def entry(*, ctype=None, pos=None, source=None, confidence=None):
    grammar = {"pos": pos or [], "ctype": ctype, "inflections": None}
    if source is not None:
        grammar["ctype_source"] = source
    if confidence is not None:
        grammar["ctype_confidence"] = confidence
    return {"uuid": "test-uuid", "entry": "試す", "grammar": grammar}


class MappingTests(unittest.TestCase):
    def test_modern_mappings(self):
        cases = {
            "動詞-五段-カ行": "五段-カ行",
            "動詞-五段-ウ行": "五段-ワア行",
            "動詞-五段-行く": "五段-カ行",
            "動詞-五段-ある": "五段-ラ行",
            "動詞-五段-ラ行-不規則": "五段-ラ行",
            "動詞-五段-ウ行-特殊": "五段-ワア行",
            "Godan verb with 'tsu' ending": "五段-タ行",
            "動詞-一段": "一段",
            "動詞-一段-くれる": "一段",
            "動詞-サ変-する": "サ行変格",
            "動詞-ずる変": "サ行変格",
            "動詞-来る": "カ行変格",
            "形容詞": "形容詞",
            "形容詞-良い型": "形容詞",
        }
        for pos, expected in cases.items():
            with self.subTest(pos=pos):
                self.assertEqual(backfill.ctype_from_pos(pos), expected)

    def test_classical_mappings(self):
        cases = {
            "動詞-四段-ラ行-古典": "文語四段-ラ行",
            "Yodan verb with 'ku' ending (archaic)": "文語四段-カ行",
            "Nidan verb (lower class) with 'ru' ending (archaic)": "文語下二段-ラ行",
            "Nidan verb (upper class) with 'bu' ending (archaic)": "文語上二段-バ行",
            "Nidan verb (lower class) with 'u' ending and 'we' conjugation (archaic)": "文語下二段-ワ行",
            "動詞-二段-ウ行-古典": "文語下二段-ア行",
            "動詞-り変": "文語ラ行変格",
            "動詞-ぬ変": "文語ナ行変格",
            "'ku' adjective (archaic)": "文語形容詞-ク",
            "'shiku' adjective (archaic)": "文語形容詞-シク",
            "archaic/formal form of na-adjective": "文語形容動詞-ナリ",
            "形容詞-たる": "文語形容動詞-タリ",
        }
        for pos, expected in cases.items():
            with self.subTest(pos=pos):
                self.assertEqual(backfill.ctype_from_pos(pos), expected)

    def test_generic_labels_do_not_infer(self):
        for pos in ("動詞", "助動詞", "形容動詞", "動詞-他動詞", "名詞"):
            with self.subTest(pos=pos):
                self.assertIsNone(backfill.ctype_from_pos(pos))

    def test_existing_alias_and_unknown_normalization(self):
        self.assertEqual(backfill.normalize_existing_ctype("  五段 - ウ行  "), ("五段-ワア行", True))
        self.assertEqual(backfill.normalize_existing_ctype(" 未知  活用 "), ("未知 活用", False))


class ProcessingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp.name) / "data"
        self.data_dir.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, value):
        path = self.data_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return path

    def test_existing_wins_and_provenance_is_preserved(self):
        item = entry(ctype=" 五段 - ウ行 ", pos=["動詞-一段"], source="manual", confidence="low")
        path = self.write("entry.json", [item])
        report = backfill.process_data(self.data_dir, apply=True)
        saved = json.loads(path.read_text())[0]["grammar"]
        self.assertEqual(saved["ctype"], "五段-ワア行")
        self.assertEqual(saved["ctype_source"], "manual")
        self.assertEqual(saved["ctype_confidence"], "low")
        self.assertEqual(report["summary"]["existing_ctype"], 1)

    def test_unknown_existing_is_retained_with_medium_confidence(self):
        path = self.write("entry.json", [entry(ctype="独自活用", pos=["動詞-一段"])])
        report = backfill.process_data(self.data_dir, apply=True)
        saved = json.loads(path.read_text())[0]["grammar"]
        self.assertEqual(saved["ctype"], "独自活用")
        self.assertEqual(saved["ctype_source"], "existing")
        self.assertEqual(saved["ctype_confidence"], "medium")
        self.assertEqual(report["summary"]["unknown_existing"], 1)

    def test_existing_source_gets_confidence_from_value(self):
        path = self.write(
            "entry.json",
            [entry(ctype="五段-カ行", pos=["動詞-一段"], source="existing", confidence="low")],
        )
        backfill.process_data(self.data_dir, apply=True)
        saved = json.loads(path.read_text())[0]["grammar"]
        self.assertEqual(saved["ctype_source"], "existing")
        self.assertEqual(saved["ctype_confidence"], "high")

    def test_same_candidate_fills_and_conflict_stays_null(self):
        same = entry(pos=["動詞-他動詞", "動詞-五段-カ行", "動詞-五段-行く"])
        conflict = entry(pos=["動詞-五段-カ行", "動詞-一段"])
        conflict["uuid"] = "conflict"
        conflict["entry"] = "衝突"
        path = self.write("entries.json", [same, conflict])
        report = backfill.process_data(self.data_dir, apply=True)
        saved = json.loads(path.read_text())
        self.assertEqual(saved[0]["grammar"]["ctype"], "五段-カ行")
        self.assertEqual(saved[0]["grammar"]["ctype_source"], "pos-derived")
        self.assertEqual(saved[0]["grammar"]["ctype_confidence"], "high")
        self.assertIsNone(saved[1]["grammar"]["ctype"])
        self.assertNotIn("ctype_source", saved[1]["grammar"])
        self.assertEqual(report["summary"]["conflicts"], 1)
        self.assertEqual(report["conflicts"][0]["candidates"], ["一段", "五段-カ行"])

    def test_null_provenance_is_removed(self):
        item = entry(pos=["動詞"], source="pos-derived", confidence="high")
        path = self.write("entry.json", item)
        backfill.process_data(self.data_dir, apply=True)
        saved = json.loads(path.read_text())["grammar"]
        self.assertIsNone(saved["ctype"])
        self.assertNotIn("ctype_source", saved)
        self.assertNotIn("ctype_confidence", saved)

    def test_dry_run_does_not_write_and_apply_is_idempotent(self):
        path = self.write("entry.json", [entry(pos=["動詞-一段"])])
        before = path.read_bytes()
        dry = backfill.process_data(self.data_dir)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(dry["summary"]["fillable_ctype"], 1)
        self.assertEqual(dry["summary"]["affected_files"], 1)

        applied = backfill.process_data(self.data_dir, apply=True)
        self.assertEqual(applied["summary"]["files_written"], 1)
        after_first = path.read_bytes()
        rerun = backfill.process_data(self.data_dir, apply=True)
        self.assertEqual(path.read_bytes(), after_first)
        self.assertEqual(rerun["summary"]["affected_files"], 0)
        self.assertEqual(rerun["summary"]["files_written"], 0)

    def test_invalid_data_is_reported(self):
        self.write("bad-root.json", 42)
        self.write("bad-entry.json", [{"uuid": "x", "entry": "x", "grammar": {"pos": "動詞", "ctype": None}}])
        (self.data_dir / "broken.json").write_text("{", encoding="utf-8")
        report = backfill.process_data(self.data_dir)
        self.assertEqual(report["summary"]["invalid_data"], 3)
        self.assertEqual(len(report["invalid_data"]), 3)

    def test_cli_defaults_to_dry_run_and_writes_report(self):
        path = self.write("entry.json", [entry(pos=["動詞-一段"])])
        before = path.read_bytes()
        report_path = Path(self.temp.name) / "report.json"
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--data-dir", str(self.data_dir), "--report", str(report_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("dry-run", result.stdout)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(json.loads(report_path.read_text())["summary"]["fillable_ctype"], 1)

    def test_cli_apply_atomically_replaces_file_and_preserves_mode(self):
        path = self.write("entry.json", [entry(pos=["動詞-一段"])])
        path.chmod(0o640)
        inode_before = path.stat().st_ino
        subprocess.run(
            [sys.executable, str(SCRIPT), "--data-dir", str(self.data_dir), "--apply"],
            check=True,
            capture_output=True,
            text=True,
        )
        saved = json.loads(path.read_text())[0]["grammar"]
        self.assertEqual(saved["ctype"], "一段")
        self.assertEqual(saved["ctype_source"], "pos-derived")
        self.assertNotEqual(path.stat().st_ino, inode_before)
        self.assertEqual(path.stat().st_mode & 0o777, 0o640)
        self.assertEqual([p for p in self.data_dir.iterdir() if p.name.startswith(".entry.json.")], [])


if __name__ == "__main__":
    unittest.main()
