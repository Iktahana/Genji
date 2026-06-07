
import json
import subprocess
import time

# 語彙リスト
vocab_list = [
    {"index": "1", "entry": "意見が一致", "reading": "いけんがいっち", "pos": "表現,名詞,動詞-サ変", "gloss": "seeing eye to eye"},
    {"index": "2", "entry": "意見がまとまる", "reading": "いけんがまとまる", "pos": "表現,動詞-五段-ラ行", "gloss": "to agree on"},
    {"index": "3", "entry": "意見具申", "reading": "いけんぐしん", "pos": "名詞,動詞-サ変", "gloss": "offering one's opinion (to someone on something)"},
    {"index": "4", "entry": "意見交換", "reading": "いけんこうかん", "pos": "名詞", "gloss": "exchange of ideas"},
    {"index": "5", "entry": "意見広告", "reading": "いけんこうこく", "pos": "名詞", "gloss": "protest advertising (on an issue by a pressure group)"},
    {"index": "6", "entry": "意見書", "reading": "いけんしょ", "pos": "名詞", "gloss": "opinion in writing"},
    {"index": "7", "entry": "違憲性", "reading": "いけんせい", "pos": "名詞", "gloss": "unconstitutionality"},
    {"index": "8", "entry": "違憲立法審査権", "reading": "いけんりっぽうしんさけん", "pos": "名詞", "gloss": "power of judicial review"},
    {"index": "9", "entry": "意見を言う", "reading": "いけんをいう", "pos": "表現,動詞-五段-ウ行", "gloss": "to state one's opinion"},
    {"index": "10", "entry": "意見を述べる", "reading": "いけんをのべる", "pos": "表現,動詞-一段", "gloss": "to state one's opinion"},
    {"index": "11", "entry": "意見を吐く", "reading": "いけんをはく", "pos": "表現,動詞-五段-カ行", "gloss": "to give one's opinion"},
    {"index": "12", "entry": "意見を持つ", "reading": "いけんをもつ", "pos": "表現,Godan verb with 'tsu' ending", "gloss": "to hold an opinion"},
    {"index": "13", "entry": "イケール", "reading": "イケール", "pos": "名詞", "gloss": "angle plate"},
    {"index": "14", "entry": "井桁", "reading": "いげた", "pos": "名詞", "gloss": "well curb consisting of wooden beams crossed at the ends"},
    {"index": "15", "entry": "井桁", "reading": "いげた", "pos": "名詞", "gloss": "pattern resembling the symbol #"},
    {"index": "16", "entry": "井桁", "reading": "いげた", "pos": "名詞", "gloss": "number sign"},
    {"index": "17", "entry": "威厳", "reading": "いげん", "pos": "名詞", "gloss": "dignity"},
    {"index": "18", "entry": "異言", "reading": "いげん", "pos": "名詞", "gloss": "tongues (i.e. utterances or "languages" spoken during instances of glossolalia)"},
    {"index": "19", "entry": "移弦", "reading": "いげん", "pos": "名詞,動詞-サ変", "gloss": "string-crossing (violin, cello, etc.)"},
    {"index": "20", "entry": "遺言", "reading": "いげん", "pos": "名詞", "gloss": "will"},
    {"index": "21", "entry": "異言語", "reading": "いげんご", "pos": "表現", "gloss": "another language"},
    {"index": "22", "entry": "医原性", "reading": "いげんせい", "pos": "名詞,名詞-の形容詞", "gloss": "iatrogenic (disease or condition caused by medical treatment)"},
    {"index": "23", "entry": "威厳のある", "reading": "いげんのある", "pos": "表現,形容詞-語幹", "gloss": "dignified"},
    {"index": "24", "entry": "医原病", "reading": "いげんびょう", "pos": "名詞", "gloss": "iatrogenic disease"},
    {"index": "25", "entry": "異言を語る", "reading": "いげんをかたる", "pos": "表現,動詞-五段-ラ行", "gloss": "to speak in tongues"},
    {"index": "26", "entry": "遺孤", "reading": "いこ", "pos": "名詞", "gloss": "orphan"},
    {"index": "27", "entry": "依估", "reading": "いこ", "pos": "名詞", "gloss": "unfairness"},
    {"index": "28", "entry": "憩い", "reading": "いこい", "pos": "名詞,名詞-の形容詞", "gloss": "rest"},
    {"index": "29", "entry": "憩いの場", "reading": "いこいのば", "pos": "名詞", "gloss": "place for relaxation and refreshment"},
    {"index": "30", "entry": "以降", "reading": "いこう", "pos": "名詞,副詞", "gloss": "on and after"},
    {"index": "31", "entry": "偉功", "reading": "いこう", "pos": "名詞", "gloss": "great deed"},
    {"index": "32", "entry": "偉効", "reading": "いこう", "pos": "名詞", "gloss": "great effect"},
    {"index": "33", "entry": "威光", "reading": "いこう", "pos": "名詞", "gloss": "power"},
    {"index": "34", "entry": "移行", "reading": "いこう", "pos": "名詞,動詞-サ変,動詞-他動詞,動詞-自動詞", "gloss": "transition"},
    {"index": "35", "entry": "移行", "reading": "いこう", "pos": "名詞,動詞-サ変,動詞-他動詞,動詞-自動詞", "gloss": "transfer (of powers, weight, etc.)"},
    {"index": "36", "entry": "移項", "reading": "いこう", "pos": "名詞,動詞-サ変,動詞-他動詞", "gloss": "transposition"},
    {"index": "37", "entry": "衣桁", "reading": "いこう", "pos": "名詞", "gloss": "clothes rack"}
]

# プロンプト作成
word_lines = "
".join([
    f"{item['index']}. 表記:{item['entry']} 読み:{item['reading']} 品詞:{item['pos']} 意味:{item['gloss']}"
    for item in vocab_list
])

prompt = (
    '各語に自然な例文を3個、JSON出力。キー="1","2",...、値=[{"text":"例文"}]。'
    '各例文に必ず見出し語（表記そのもの、または自然な活用形）を含めること。'
    '具体的な場面の文にする。メタ説明文（「◯◯という言葉は重要」「◯◯の意味を理解している」「この文には◯◯が含まれる」等）やテンプレ文は禁止。'
    '感動詞は会話文にする。

'
    f'{word_lines}

'
    '{"1":[{"text":"..."}],"2":[{"text":"..."}]}'
)

print(prompt)
