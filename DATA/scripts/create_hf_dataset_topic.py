import pandas as pd
from datasets import Dataset
import os

CSV_PATH = "C:/Users/user/Documents/sp26/nlp/proj/DATA/raw/topics_cleaned.csv"
OUTPUT_DIR = "hf_task_dataset"

def load_csv(path):
    df = pd.read_csv(path)
    df = df.dropna()
    return Dataset.from_pandas(df)

def build_task_dataset(csv_path):
    return load_csv(csv_path)

if __name__ == "__main__":
    dataset = build_task_dataset(CSV_PATH)

    dataset.save_to_disk(OUTPUT_DIR)
    print(dataset)