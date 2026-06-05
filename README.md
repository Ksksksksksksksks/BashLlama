# BashLlama: Adapting Llama‑2‑7B to Bashkir

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

This repository contains the code, datasets, and experimental logs for the first systematic adaptation of Llama‑2‑7B to the **Bashkir language** (a low‑resource Turkic language spoken by ~1.4 million people).  
The project implements vocabulary extension, optional embedding alignment via a Russian–Bashkir parallel corpus, continual training (LoRA, 4‑bit QLoRA), and task‑specific fine‑tuning for topic classification.  
We release all datasets, trained model artifacts, and Weights & Biases logs to support further research on low‑resource language adaptation.

---

## 📌 Key Results

- **Vocabulary extension** → 28 600 new tokens, final vocabulary size 60 379, no `[UNK]` on Bashkir texts.
- **Continual training** (3000 steps) →  
  - Model A (no alignment): final loss **1.344**  
  - Model B (with Russian–Bashkir embedding alignment): final loss **1.318**  
- **Topic classification** (10 classes, 267 headlines):
  - Fine‑tuned accuracy: baseline 24.1%, A 24.1%, B 20.4%
  - **Few‑shot (3‑shot) accuracy**: B **18.5%** > A 16.7% > baseline 14.8% (**+25% relative improvement**)
- Alignment improves few‑shot generalisation and accelerates convergence.

---

## 📂 Repository Structure

```
BashLlama/
├── notebooks/                     # Jupyter notebooks for all pipeline stages
│   ├── smoke_test.ipynb
│   ├── vocab_extension.ipynb
│   ├── embedding_alignment.ipynb
│   ├── continual_training_A.ipynb
│   ├── continual_training_B.ipynb
│   ├── instruction_tuning.ipynb      # data ready, training not executed
│   ├── task_specific_tuning.ipynb                  
├── DATA/                          # data scraping and preprocessing
│   ├─scripts/
```

---

## 💾 Datasets (Hugging Face)

All datasets are publicly available under the `metuKKhud` namespace:

| Dataset | HF Link | Size / Description |
|---------|---------|--------------------|
| **Raw Bashkir Corpus** (non‑shuffled) | [metuKKhud/bashqort-raw](https://huggingface.co/datasets/metuKKhud/bashqort-raw) | 63 270 documents, ~21M tokens (news + public domain) |
| **Bashkir Alpaca** (instruction set) | [metuKKhud/bashqort-alpaca](https://huggingface.co/datasets/metuKKhud/bashqort-alpaca) | 52 k instruction‑response pairs (machine‑translated from Russian Alpaca, validated by native speakers) |
| **Topic Classification** | [metuKKhud/bashqort-task](https://huggingface.co/datasets/metuKKhud/bashqort-task) | 267 headlines, 10 topics |
| **Russian–Bashkir Parallel Corpus** (external) | [AigizK/bashkir-russian-parallel-corpora](https://huggingface.co/datasets/AigizK/bashkir-russian-parallel-corpora) | 1.2M sentence pairs, CC‑BY‑4.0 |

---

## 🔬 Weights & Biases Logs

Every experiment stage is logged in Weights & Biases. View the dashboards:

| Stage | W&B Project Link |
|-------|-------------------|
| Vocabulary extension | [bashllama-vocab-extension](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-vocab-extension) |
| Embedding alignment (Exp. B) | [bashllama-embedding-alignment](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-embedding-alignment) |
| Continual training A | [bashllama-continual-training-A](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-continual-training-A) |
| Continual training B | [bashllama-continual-training-B](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-continual-training-B) |
| Task‑specific tuning | [bashllama-task-specific](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-task-specific) |

> All trained models are also stored as W&B artifacts (e.g., `llama2_bashkir_continual_A`, `llama2_bashkir_continual_B`).

---

## 🚀 Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/Ksksksksksksksks/BashLlama.git
cd BashLlama
```

### 2. Launch a notebook
Open any notebook from `notebooks/` in Kaggle or locally with a GPU (Tesla T4 or similar, ≥15GB VRAM).  
For Kaggle, set **Accelerator = GPU T4** and **Internet = On**.

### 3. Reproduce the pipeline
Run the notebooks in order:
- `vocab_extension.ipynb` – train BPE tokenizer, extend vocabulary, warm‑up embeddings.
- `embedding_alignment.ipynb` – (optional, for Experiment B) align embeddings using the parallel corpus.
- `continual_training_A.ipynb` and/or `03_continual_training_B.ipynb` – continual training (3000 steps).
- `task_specific_tuning.ipynb` – fine‑tune for topic classification and evaluate zero‑/few‑shot.

> Instruction tuning is not executed but the dataset is ready; you can run it if desired.

---

## 📖 Citation

If you use this code, datasets, or models in your research, please cite:

```bibtex
@misc{khudiakova2026bashllama,
  author       = {Kseniia Khudiakova},
  title        = {BashLlama: Adapting Llama-2-7B to the Bashkir Language},
  year         = {2026},
  howpublished = {GitHub repository},
  url          = {https://github.com/Ksksksksksksksks/BashLlama}
}
```

Also consider citing the foundational works:
```
@article{toraman2024llamaturk,
  title     = {LlamaTurk: Adapting Open-Source Generative Large Language Models for Low-Resource Languages},
  author    = {Toraman, Cagri},
  journal   = {arXiv preprint arXiv:2405.07745},
  year      = {2024},
  url       = {https://arxiv.org/abs/2405.07745}
}

@article{mahdizadeh2024extending,
  title     = {Extending LLMs to new languages: A case study of Llama and Persian adaptation},
  author    = {Mahdizadeh Sani, Samin and Sadeghi, Pouya and Vu, Thuy-Trang and Yaghoobzadeh, Yadollah and Haffari, Gholamreza},
  journal   = {arXiv preprint arXiv:2412.13375},
  year      = {2024},
  url       = {https://arxiv.org/abs/2412.13375}
}
```

---

## 🙏 Acknowledgements

- **Ilyas Khatipov** ([GitHub](https://github.com/IlyasKhatipov)) – annotation of the topic classification dataset and validation of Bashkir Alpaca translations.
- **Azamat Kireev** ([GitHub](https://github.com/danzyxd)) – validation of Bashkir Alpaca translations.
- The authors of LlamaTurk and the Persian adaptation work for their open frameworks.
- Hugging Face, Kaggle, and Weights & Biases for their free tiers that made this research possible.

---

## 📜 License

The code in this repository is released under the **MIT License**.  
Datasets are released under CC‑BY‑4.0 (parallel corpus) or MIT (our datasets). See individual dataset pages for details.

---

**For questions or suggestions, please open an issue or contact** [e278979@metu.edu.tr](mailto:e278979@metu.edu.tr).
