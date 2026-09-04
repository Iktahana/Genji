import importlib.util
import sqlite3
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "script"
sys.path.insert(0, str(SCRIPT_DIR))
SPEC = importlib.util.spec_from_file_location("json_to_sqlite", SCRIPT_DIR / "json_to_sqlite.py")
assert SPEC and SPEC.loader
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


class SQLiteCtypeTests(unittest.TestCase):
    def test_schema_and_insert_include_ctype_provenance(self):
        conn = sqlite3.connect(":memory:")
        builder.create_schema(conn)
        item = {
            "uuid": "00000000-0000-0000-0000-000000000001",
            "entry": "歩く",
            "reading": {"primary": "あるく", "alternatives": [], "is_heteronym": False},
            "grammar": {
                "pos": ["動詞-五段-カ行"],
                "ctype": "五段-カ行",
                "ctype_source": "pos-derived",
                "ctype_confidence": "high",
                "inflections": None,
            },
            "definitions": [],
            "relations": {},
            "meta": {},
        }
        builder.insert_entry(conn, item)

        columns = {row[1] for row in conn.execute("PRAGMA table_info(entries)")}
        self.assertIn("ctype_source", columns)
        self.assertIn("ctype_confidence", columns)
        self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 3)
        row = conn.execute(
            "SELECT ctype, ctype_source, ctype_confidence FROM entries"
        ).fetchone()
        self.assertEqual(row, ("五段-カ行", "pos-derived", "high"))

    def test_metadata_schema_version_is_three(self):
        conn = sqlite3.connect(":memory:")
        builder.create_metadata(conn, 0)
        version = conn.execute(
            "SELECT value FROM _metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        self.assertEqual(version, "3")


if __name__ == "__main__":
    unittest.main()
