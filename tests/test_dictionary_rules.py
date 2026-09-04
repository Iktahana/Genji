import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "script"))

from dictionary_rules import (  # noqa: E402
    expected_data_path,
    invalid_reading_chars,
    is_valid_reading,
    reading_bucket,
    reading_to_file_key,
)
from check_data_quality import _deduplicate_rows, audit  # noqa: E402


class DictionaryRulesTest(unittest.TestCase):
    def test_accepts_unicode_kana_and_japanese_reading_marks(self):
        for reading in (
            "あい", "アイスクリーム", "ぷちヽヽ", "ガレット・デ・ロワ", "ﾀﾋ",
            "ｶﾞｯﾂ", "ゟ", "こと、もの", "モワァ〜ン", "オビ＝ワン・ケノービ",
        ):
            self.assertTrue(is_valid_reading(reading), reading)

    def test_rejects_han_latin_digits_punctuation_and_spacing_dakuten(self):
        for reading in ("阿輩だい", "於蘭", "え゛りと", "゙あ", "あ゙゙", "abc", "123"):
            self.assertFalse(is_valid_reading(reading), reading)
            self.assertTrue(invalid_reading_chars(reading))

    def test_builds_canonical_data_path(self):
        root = Path("/repo/data")
        self.assertEqual(expected_data_path(root, "あい"), root / "あ" / "アイ.json")
        self.assertEqual(expected_data_path(root, "プラン"), root / "ぷ" / "プラン.json")
        self.assertEqual(expected_data_path(root, "ﾀﾋ"), root / "た" / "タヒ.json")
        self.assertIsNone(expected_data_path(root, "阿輩だい"))

    def test_quarantines_unresolved_reading_without_touching_valid_entry(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            data = base / "data"
            pending = base / "pending" / "needs_reading"
            path = data / "あ" / "アイ.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                '[{"uuid":"ae8b2014-c9cd-546a-a100-faf6aaaeb964",'
                '"entry":"愛","reading":{"primary":"あい","alternatives":[]},'
                '"definitions":[{"index":1}]},'
                '{"uuid":"97408ef0-29f6-50ec-9b9a-8f6cfb77a587",'
                '"entry":"於蘭","reading":{"primary":"於蘭","alternatives":[]},'
                '"definitions":[{"index":1}],"meta":{"needs_reading":true}}]',
                encoding="utf-8",
            )

            result = audit(data, pending, fix=True)
            self.assertEqual(result.quarantined, 1)
            self.assertIn('"entry": "愛"', path.read_text(encoding="utf-8"))
            quarantined = list(pending.rglob("*.json"))
            self.assertEqual(len(quarantined), 1)
            self.assertIn('"entry": "於蘭"', quarantined[0].read_text(encoding="utf-8"))

    def test_duplicate_uuid_merges_distinct_senses_and_reindexes(self):
        common = {
            "uuid": "25ae3663-72f9-5648-ac26-5dbeeae9f102",
            "entry": "アイドル",
            "reading": {"primary": "アイドル", "alternatives": []},
            "grammar": {"pos": ["名詞"]},
            "relations": {"related": []},
            "meta": {},
        }
        idol = {**common, "definitions": [{
            "index": 1, "gloss": "idol", "register": "standard",
            "examples": {"standard": [{"text": "idol"}], "literary": []},
        }]}
        idle = {
            **common,
            "grammar": {"pos": ["形容詞-語幹"]},
            "definitions": [{
                "index": 1, "gloss": "idle", "register": "standard",
                "examples": {"standard": [{"text": "idle"}], "literary": []},
            }],
        }

        merged, removed = _deduplicate_rows([idol, idle])
        self.assertEqual(removed, 1)
        self.assertEqual([sense["gloss"] for sense in merged[0]["definitions"]], ["idol", "idle"])
        self.assertEqual([sense["index"] for sense in merged[0]["definitions"]], [1, 2])
        self.assertCountEqual(merged[0]["grammar"]["pos"], ["名詞", "形容詞-語幹"])


if __name__ == "__main__":
    unittest.main()
