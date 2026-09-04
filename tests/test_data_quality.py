import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "script"))

from check_data_quality import _json_report, audit  # noqa: E402
from dictionary_rules import compute_uuid_v5, expected_data_path  # noqa: E402
from migrate_uuids import apply_mapping, build_mapping  # noqa: E402
from promote_pending import promote  # noqa: E402


def item(entry="愛", reading="あい"):
    return {
        "uuid": compute_uuid_v5(entry, reading),
        "entry": entry,
        "reading": {"primary": reading, "alternatives": [], "is_heteronym": False},
        "grammar": {"pos": ["名詞"], "ctype": None, "inflections": None},
        "definitions": [{
            "index": 1,
            "gloss": "love",
            "register": "standard",
            "examples": {"standard": [{"text": "愛を語る。"}], "literary": []},
        }],
        "relations": {"homophones": [], "synonyms": [], "antonyms": [], "related": []},
        "meta": {"version": "1.0.0", "source": "test"},
    }


def write_row(root, row):
    path = expected_data_path(root, row["reading"]["primary"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([row], ensure_ascii=False), encoding="utf-8")
    return path


class AuditTests(unittest.TestCase):
    def test_fix_is_idempotent_and_keeps_richest_example_citation(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data, pending = base / "data", base / "pending"
            row = item()
            row["reading"]["alternatives"] = ["あい", "アイ", "アイ"]
            row["grammar"]["pos"] = ["名詞", "名詞"]
            row["definitions"][0]["index"] = 9
            row["definitions"][0]["examples"]["standard"] = [
                {"text": " 愛を語る。 ", "citation": {"source": "short"}},
                {"text": "愛を語る。", "citation": {"source": "rich", "author": "writer"}},
            ]
            path = write_row(data, row)

            first = audit(data, pending, fix=True)
            fixed = json.loads(path.read_text(encoding="utf-8"))[0]
            self.assertGreater(first.changed_files, 0)
            self.assertEqual(first.examples_deduplicated, 1)
            self.assertEqual(fixed["reading"]["alternatives"], ["アイ"])
            self.assertEqual(fixed["grammar"]["pos"], ["名詞"])
            self.assertEqual(fixed["definitions"][0]["index"], 1)
            examples = fixed["definitions"][0]["examples"]["standard"]
            self.assertEqual(len(examples), 1)
            self.assertEqual(examples[0]["citation"]["author"], "writer")

            second = audit(data, pending, fix=True)
            self.assertEqual(second.changed_files, 0)
            self.assertEqual(second.examples_deduplicated, 0)

    def test_pending_marker_is_required_and_fixed(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data, pending = base / "data", base / "pending"
            path = pending / "U+6100-U+61FF" / "愛.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps([item()], ensure_ascii=False), encoding="utf-8")
            before = audit(data, pending)
            self.assertEqual(before.issue_counts["reading.pending_marker_missing"], 1)
            formal_only = audit(data, pending, include_pending=False)
            self.assertEqual(formal_only.error_count, 0)
            self.assertEqual(formal_only.scope_entries["pending"], 0)
            audit(data, pending, fix=True)
            fixed = json.loads(path.read_text(encoding="utf-8"))[0]
            self.assertIs(fixed["meta"]["needs_reading"], True)

    def test_hard_schema_rules_and_grouped_json(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data, pending = base / "data", base / "pending"
            row = item()
            row["definitions"][0]["gloss"] = 3
            row["definitions"][0]["examples"]["standard"] = [{"text": ""}]
            write_row(data, row)
            result = audit(data, pending)
            self.assertGreater(result.error_count, 0)
            report = _json_report(result)
            self.assertIn("error", report["issues"])
            self.assertIn("schema.definition.gloss", report["issues"]["error"])
            self.assertIn("data", report["issues"]["error"]["schema.definition.gloss"])

    def test_detects_casefold_path_collision(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data, pending = base / "data", base / "pending"
            directory = data / "あ"
            directory.mkdir(parents=True)
            (directory / "A.json").write_text(json.dumps([item()], ensure_ascii=False), encoding="utf-8")
            (directory / "Ａ.json").write_text(json.dumps([item("藍", "あお")], ensure_ascii=False), encoding="utf-8")
            result = audit(data, pending)
            self.assertEqual(result.issue_counts["path.portable_collision"], 1)


class MigrationTests(unittest.TestCase):
    def test_uuid_mapping_precedes_apply_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data, pending = base / "data", base / "pending"
            row = item()
            row["uuid"] = "11111111-1111-5111-8111-111111111111"
            path = write_row(data, row)
            mapping = build_mapping(data, pending)
            self.assertEqual(len(mapping), 1)
            self.assertEqual(mapping[0]["path"], str(path))
            self.assertEqual(mapping[0]["entry"], "愛")
            self.assertEqual(mapping[0]["reading"], "あい")
            self.assertEqual(apply_mapping(mapping), 1)
            self.assertEqual(build_mapping(data, pending), [])

    def test_promotes_only_reviewed_and_uuid_migrated_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data, pending = base / "data", base / "pending"
            source = pending / "U+6100-U+61FF" / "愛.json"
            source.parent.mkdir(parents=True)
            source.write_text(json.dumps([item()], ensure_ascii=False), encoding="utf-8")
            dry_run = promote(data, pending, [], apply=False)
            self.assertEqual(dry_run["eligible"], 1)
            self.assertTrue(source.exists())
            applied = promote(data, pending, [], apply=True)
            self.assertEqual(applied["promoted"], 1)
            self.assertFalse(source.exists())
            self.assertTrue(expected_data_path(data, "あい").exists())


if __name__ == "__main__":
    unittest.main()
