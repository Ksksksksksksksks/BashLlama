import os
from dotenv import load_dotenv
from datasets import load_from_disk
from huggingface_hub import whoami


load_dotenv()

HF_TOKEN = os.getenv("HF_TOKEN")
HF_USERNAME = os.getenv("HF_USERNAME")

print(whoami())

DATASETS = {
    # "bashqort-raw": "hf_bashkir_raw",
    # "bashqort-task": "hf_task_dataset",
    "bashqort-alpaca": "hf_bashkir_alpaca",
}

def push_dataset(local_path, repo_name):
    print(f"\nUploading {repo_name}...")

    dataset = load_from_disk(local_path)

    dataset.push_to_hub(
        f"{HF_USERNAME}/{repo_name}",
        token=HF_TOKEN
    )

    print(f"Done: {repo_name}")


if __name__ == "__main__":
    for name, path in DATASETS.items():
        push_dataset(path, name)