import json
import re
from pathlib import Path

RAW_DIR = Path("../raw/bashkir-corpus/shuffled_texts")
OUT_DIR = Path("../processed")

OUT_DIR.mkdir(exist_ok=True)

SOURCES = [
    "texts-bashdram",
    "texts-bashgazet",
    "texts-gsrb",
    "texts-jeshlek",
    "texts-kiskeufa",
    "texts-kulturarb",
    "texts-president-rb",
    "texts-tabin"
]

MIN_TEXT_LEN = 50


def clean_text(text):
    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = text.replace("\u200b", "")
    
    # remove slashes and backslashes
    text = text.replace("/", " ")
    text = text.replace("\\", " ")

    # normalize spaces
    text = re.sub(r"[ \t]+", " ", text)

    # normalize newlines
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def process_source(source_name):
    input_path = RAW_DIR / source_name
    output_path = OUT_DIR / f"{source_name}_clean.jsonl"

    seen = set()
    saved = 0
    skipped = 0

    txt_files = list(input_path.glob("*.txt"))
    print(f"{source_name}: found {len(txt_files)} txt files")

    with open(output_path, "w", encoding="utf-8") as fout:
        for txt_file in txt_files:
            try:
                with open(txt_file, encoding="utf-8") as fin:
                    text = fin.read()

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
                print(f"ERROR processing {txt_file}: {e}")
                skipped += 1

    print(f"{source_name}: saved={saved}, skipped={skipped}")


if __name__ == "__main__":
    for source in SOURCES:
        process_source(source)

    print("DONE")
