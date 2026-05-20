import json
from pathlib import Path

PROCESSED_DIR = Path("../processed")


def get_source_from_filename(filename):
    """Extract source name from filename (remove _clean.jsonl suffix, texts, articles)"""
    source = filename.replace("_clean.jsonl", "")
    source = source.replace("texts-", "")
    source = source.replace("_articles", "")
    return source


def is_shuffled(filename):
    """Determine if file is shuffled based on filename"""
    # Files with "articles" in name or public_domain are NOT shuffled
    if "articles" in filename or filename == "public_domain_clean.jsonl":
        return False
    return True


def process_file(filename):
    input_path = PROCESSED_DIR / filename
    source = get_source_from_filename(filename)
    shuffled = is_shuffled(filename)

    print(f"Processing {filename}: source={source}, is_shuffled={shuffled}")

    # Read all lines
    lines = []
    with open(input_path, encoding="utf-8") as fin:
        for line in fin:
            try:
                item = json.loads(line)
                # Add new fields
                item["source"] = source
                item["is_shuffled"] = shuffled
                lines.append(json.dumps(item, ensure_ascii=False))
            except Exception as e:
                print(f"ERROR parsing line: {e}")

    # Write back
    with open(input_path, "w", encoding="utf-8") as fout:
        for line in lines:
            fout.write(line + "\n")

    print(f"Updated {filename}: {len(lines)} lines")


if __name__ == "__main__":
    jsonl_files = list(PROCESSED_DIR.glob("*_clean.jsonl"))
    
    for file in jsonl_files:
        process_file(file.name)
    
    print("DONE")
