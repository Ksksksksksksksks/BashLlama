# Bashkir NLP Datasets

This folder contains scripts and tools for building and processing Bashkir language datasets for NLP tasks, including translation, instruction tuning, topic classification, and raw corpus processing.

---

## 📦 Datasets

### 1. Raw Bashkir Corpus
Large-scale cleaned Bashkir text corpus collected from multiple sources.

- Purpose: continued pretraining, embeddings, language modeling
- Format: JSONL / HuggingFace Dataset

🔗 https://huggingface.co/datasets/metuKKhud/bashqort-raw

---

### 2. RU → Bashkir Parallel Corpus
Parallel dataset for machine translation (Russian → Bashkir).

- Purpose: translation models, alignment learning
- Format: JSONL / HF Dataset

🔗 https://huggingface.co/datasets/AigizK/bashkir-russian-parallel-corpora

---

### 3. Bashkir Alpaca Instruction Dataset
Instruction-tuning dataset translated and adapted from Alpaca.

- Purpose: instruction tuning, chat models
- Format: instruction / input / output

🔗 https://huggingface.co/datasets/metuKKhud/bashqort-alpaca

---

### 4. Bashkir Topic Classification Dataset
Task-specific dataset labeled by native speakers.

- Purpose: text classification, evaluation benchmark
- Format: CSV / HF Dataset

🔗 https://huggingface.co/datasets/metuKKhud/bashqort-task


---

## ⚙️ Pipeline Overview

The data pipeline consists of:

1. Raw data collection (web + public domain + GitHub corpora)
2. Cleaning & normalization & translation
3. Dataset structuring (JSONL → HuggingFace format)


---
## 📌 Notes

All datasets are hosted on HuggingFace Hub and versioned separately from the codebase.