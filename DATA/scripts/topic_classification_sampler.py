import json
import random
import pandas as pd
from pathlib import Path


def sample_titles_for_labeling(input_jsonl, output_csv, n_samples=200, seed=42):
    """
    Randomly select news topic for hand razmetka.
    """
    random.seed(seed)

    records = []
    with open(input_jsonl, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    if len(records) < n_samples:
        sampled = records
    else:
        sampled = random.sample(records, n_samples)


    df = pd.DataFrame([
        {
            "id": rec["id"],
            "title": rec["title"],
            "topic": ""
        }
        for rec in sampled
    ])

    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"Saved {len(df)} titles into {output_csv}")
    return df


if __name__ == "__main__":
    input_file = "bash_news_articles.jsonl"
    output_file = "titles_for_labeling_1.csv"

    sample_titles_for_labeling(input_file, output_file, n_samples=350)