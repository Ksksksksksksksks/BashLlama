import json
import re
from pathlib import Path

RAW_DIR = Path("raw")
OUT_DIR = Path("processed")

OUT_DIR.mkdir(exist_ok=True)

FILES = [
    "neftcity_articles.jsonl",
    "bash_news_articles.jsonl",
    "bashgazet_articles.jsonl"
]

MIN_TEXT_LEN = 50


def clean_text(text):
    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = text.replace("\u200b", "")

    # normalize spaces
    text = re.sub(r"[ \t]+", " ", text)

    # normalize newlines
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def process_file(filename):
    input_path = RAW_DIR / filename
    output_path = OUT_DIR / filename.replace(".jsonl", "_clean.jsonl")

    seen = set()
    saved = 0
    skipped = 0

    with open(input_path, encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:

        for line in fin:
            try:
                item = json.loads(line)

                text = item.get("text", "")
                text = clean_text(text)

                # remove empty / tiny texts
                if len(text) < MIN_TEXT_LEN:
                    skipped += 1
                    continue

                # dedup
                if text in seen:
                    skipped += 1
                    continue

                seen.add(text)

                out = {
                    "text": text
                }

                fout.write(json.dumps(out, ensure_ascii=False) + "\n")
                saved += 1

            except Exception as e:
                print("ERROR:", e)
                skipped += 1

    print(f"{filename}: saved={saved}, skipped={skipped}")


if __name__ == "__main__":
    for file in FILES:
        process_file(file)

    print("DONE")