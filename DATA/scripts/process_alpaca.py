from datasets import Dataset
import json
import os
import glob

RAW_DIR = "../raw"
PROCESSED_DIR = "../processed"

OUTPUT_JSONL = os.path.join(PROCESSED_DIR, "alpaca_bashkir.jsonl")
OUTPUT_HF = "../hf_bashkir_alpaca"

PART_PATTERN = "alpaca_ru_ba_part_*.jsonl"


def load_jsonl(path):
    data = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            # basic validation
            if (
                obj.get("instruction")
                and obj.get("output")
            ):
                data.append(obj)

    return data


def merge_parts():
    all_data = []

    paths = sorted(
        glob.glob(os.path.join(RAW_DIR, PART_PATTERN))
    )

    print(f"Found {len(paths)} parts")

    for path in paths:
        print(f"Loading: {path}")

        data = load_jsonl(path)

        all_data.extend(data)

        print(f"Current size: {len(all_data)}")

    return all_data


def save_jsonl(data, path):
    with open(path, "w", encoding="utf-8") as f:
        for row in data:
            f.write(
                json.dumps(row, ensure_ascii=False) + "\n"
            )


def build_hf_dataset(data):
    return Dataset.from_list(data)


if __name__ == "__main__":

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # merge
    data = merge_parts()

    print(f"\nFINAL SIZE: {len(data)}")

    # save merged jsonl
    save_jsonl(data, OUTPUT_JSONL)

    print(f"\nSaved merged jsonl:")
    print(OUTPUT_JSONL)

    # create HF dataset
    dataset = build_hf_dataset(data)

    dataset.save_to_disk(OUTPUT_HF)

    print("\nHF dataset saved:")
    print(OUTPUT_HF)

    print(dataset)