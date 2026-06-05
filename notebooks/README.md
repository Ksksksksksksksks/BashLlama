## 📓 Notebooks Overview

All notebooks are designed to run on **Kaggle with GPU T4** (15GB VRAM).  
Set **Accelerator = GPU T4** and **Internet = On** before running.

### Recommended order (full pipeline):

1. `vocab-extension.ipynb`
2. `embedding-alignment.ipynb` (only for Experiment B)
3. `continual-training_a.ipynb` and/or `continual-training_b.ipynb`
4. `task-specific-tuning_baseline.ipynb`, `task-specific-tuning_a.ipynb`, `task-specific-tuning_b.ipynb`
5. (optional) `smoke-test.ipynb` – quick sanity check before full runs.

---

### 🔧 `vocab-extension.ipynb`

**Purpose:**  
- Train a BPE tokenizer on the non‑shuffled Bashkir corpus (vocab size 28 600).  
- Merge new tokens with the original Llama‑2 vocabulary (32 000 → 60 379).  
- Perform a short warm‑up (50 steps) of the new embeddings.  
- Save the extended model and tokenizer as a W&B artifact.

**Key hyperparameters:**  
- BPE vocab size: 28 600  
- Warm‑up: 50 steps, batch size 4, LR 1e‑4  

**W&B project:** [bashllama-vocab-extension](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-vocab-extension)

---

### 🔗 `embedding-alignment.ipynb`

**Purpose (Experiment B only):**  
- Freeze all transformer layers.  
- Train only the embedding layer and `lm_head` using LoRA + `modules_to_save` on the Russian–Bashkir parallel corpus (1.2M pairs).  
- Merge the alignment adapter into the base model.  
- Save the aligned model for continual training.

**Key hyperparameters:**  
- Batch size 4, gradient accumulation 4, LR 1e‑4, ~2 epochs  
- LoRA: r=8, α=32, target modules = `[]`, modules_to_save = `["embed_tokens","lm_head"]`  

**W&B project:** [bashllama-embedding-alignment](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-embedding-alignment)

---

### 🧠 `continual-training_a.ipynb` / `continual-training_b.ipynb`

**Purpose:**  
- Load the extended model (A: from vocab extension, B: from alignment).  
- Add LoRA adapters (`q_proj`, `v_proj`).  
- Train on the non‑shuffled Bashkir corpus (63 270 texts) for 3000 steps.  
- Save checkpoints as W&B artifacts (every 500 steps) and final model.

**Key hyperparameters:**  
- LoRA: r=8, α=32, target_modules = `["q_proj","v_proj"]`  
- 4‑bit NF4 quantisation (QLoRA)  
- Batch size 4, gradient accumulation 4, effective batch 16  
- Learning rate 3e‑4, 3000 steps  

**W&B projects:**  
- A: [bashllama-continual-training-A](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-continual-training-A)  
- B: [bashllama-continual-training-B](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-continual-training-B)

---

### 🎯 Task‑specific tuning notebooks

#### `task-specific-tuning_baseline.ipynb`  
**Purpose:** Fine‑tune the baseline model (vocab extension only) on topic classification (267 headlines, 10 classes). Evaluate zero‑shot, few‑shot (3‑shot), and fine‑tuned accuracy.  

#### `task-specific-tuning_a.ipynb`  
**Purpose:** Fine‑tune Experiment A (continual training without alignment).  

#### `task-specific-tuning_b.ipynb`  
**Purpose:** Fine‑tune Experiment B (continual training with alignment).  

**Key hyperparameters (all three):**  
- LoRA: r=8, α=32, target_modules = `["q_proj","v_proj"]`  
- Batch size 4, gradient accumulation 4, LR 2e‑4, 3 epochs  
- Train split: 213 headlines, test split: 54 headlines  

**W&B project:** [bashllama-task-specific](https://wandb.ai/e278979-metu-middle-east-technical-university/bashllama-task-specific)

---

### 🔥 `smoke-test.ipynb`

**Purpose:**  
- Quick validation of the environment and pipeline before full runs.  
- Loads Llama‑2‑7B in 4‑bit, adds LoRA, runs 100 training steps on a tiny sample of the raw corpus.  
- Checks that loss decreases and generation produces Bashkir-like text.

**Use:** Run once after setting up Kaggle to ensure everything works.

---

## 🗂️ Notes

- All trained models and checkpoints are saved as **W&B artifacts**; you can download them directly from the respective W&B projects.
- The instruction tuning notebook (`04_instruction_tuning.ipynb`) is **not listed** because it was not executed (data ready, training not run due to time constraints). You can run it using the prepared `bashqort-alpaca` dataset if desired.
- For detailed hyperparameters and results, refer to the final report and the W&B dashboards.

--- 
