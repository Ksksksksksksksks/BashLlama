import json
import re
from pathlib import Path

RAW_DIR = Path("../raw/bashkir-corpus")
OUT_DIR = Path("../processed")

OUT_DIR.mkdir(exist_ok=True)

METATABLE_FILE = "public_domain_metatable.tsv"
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


def process_public_domain():
    input_path = RAW_DIR / METATABLE_FILE
    output_path = OUT_DIR / "public_domain_clean.jsonl"

    seen = set()
    saved = 0
    skipped = 0

    with open(input_path, encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:

        # skip header
        header = fin.readline()
        
        for line in fin:
            if not line.strip():
                continue

            parts = line.strip().split("\t")
            if len(parts) < 1:
                skipped += 1
                continue

            txt_path = parts[0]
            full_path = RAW_DIR / txt_path

            try:
                with open(full_path, encoding="utf-8") as txt_file:
                    text = txt_file.read()

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
                print(f"ERROR processing {txt_path}: {e}")
                skipped += 1

    print(f"public_domain: saved={saved}, skipped={skipped}")


if __name__ == "__main__":
    process_public_domain()
    print("DONE")
