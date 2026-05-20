from datasets import Dataset, DatasetDict, load_dataset
import json
import os

INPUT_DIR = "processed"
OUTPUT_DIR = "hf_bashkir_raw"

def load_jsonl(path):
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "text" in obj and obj["text"].strip():
                data.append(obj)
    return data

def clean_name(name: str):
    return name.replace("-", "_").replace(" ", "_")

def build_dataset_by_source(input_dir):
    source_groups = {}

    for root, _, files in os.walk(input_dir):
        for file in files:
            if not file.endswith(".jsonl"):
                continue

            path = os.path.join(root, file)

            source_name = file.replace("_clean.jsonl", "").replace(".jsonl", "")

            data = load_jsonl(path)

            if source_name not in source_groups:
                source_groups[source_name] = []

            source_groups[source_name].extend(data)

    datasets_dict = {}

    for source, data in source_groups.items():
        datasets_dict[clean_name(source)] = Dataset.from_list(data)

    return DatasetDict(datasets_dict)


if __name__ == "__main__":
    dataset = build_dataset_by_source(INPUT_DIR)

    dataset.save_to_disk(OUTPUT_DIR)
    print(dataset)