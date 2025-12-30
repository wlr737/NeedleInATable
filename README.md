Here is the updated `README.md` tailored to your GitHub project. I have added the **Project Structure Analysis** and the **Data Disclaimer** as requested.

***

# NEEDLEINATABLE: Exploring Long-Context Capability of Large Language Models towards Long-Structured Tables

[![arXiv](https://img.shields.io/badge/arXiv-2504.06560-b31b1b.svg)](https://arxiv.org/abs/2504.06560)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This repository contains the official implementation and data for the paper **"NEEDLEINATABLE: Exploring Long-Context Capability of Large Language Models towards Long-Structured Tables"**, accepted at **NeurIPS 2025**.

## 📖 Overview

**NEEDLEINATABLE (NIAT)** is a benchmark designed to evaluate the fine-grained perception of Large Language Models (LLMs) regarding individual table cells within long-context structured tables. Unlike previous benchmarks that focus on high-level reasoning or unstructured text, NIAT treats each cell as a "needle" to test the model's fundamental understanding of table structures.

We also introduce a **Strong2Weak** data synthesis method, using GPT-4o to generate Chain-of-Thought (CoT) training data, which significantly improves open-source model performance on both NIAT and downstream tabular tasks.

## ⚠️ Data Availability Note

> **Note:** Due to the massive size of the original QA (Question-Answer) dataset (up to 287K questions), we have uploaded **all original raw tables** but only a **random sample of the QA pairs** in this repository for demonstration and testing purposes. 
>
> If you require the full training/testing dataset, please refer to the instructions in the `data/` folder or contact the authors.

## 📂 Project Structure

Below is an overview of the repository structure and the function of each module:

```text
NeedleInATable/
├── data_synthesis/                 # Data synthesis modules
│   ├── synthetic_table/            # Core synthesis scripts
│   │   ├── build_cell-locating.py  # Script for cell location tasks
│   │   └── build_cell-lookup.py    # Script for cell retrieval tasks
│   └── promptp_cot4_cell-locating.py # CoT prompts for location tasks
├── NIAT_data/                      # Evaluation datasets
│   ├── NIAT_LLM_test_data/         # Benchmarks for text-based LLMs
│   │   ├── cell-locating/          # Location task samples
│   │   ├── cell-lookup/            # Retrieval task samples
│   │   └── NIAT_cropped_tables.json # Cropped table data (JSON)
│   ├── NIAT_MLLM_test_data/        # Benchmarks for Vision-Language Models
│   │   ├── cropped_table_images/   # Cropped table screenshots
│   │   ├── table_images/           # Full table screenshots
│   │   ├── vlm_cell_locating.json  # VLM location annotations
│   │   ├── vlm_cell_locating_50.json # VLM location subset (50 samples)
│   │   └── vlm_cell_lookup.json    # VLM retrieval annotations
│   └── 360_NIAT_tables.json        # Main source table collection
└── README.md                       # Project documentation
```


## 📜 Citation

If you use this code or dataset in your research, please cite our paper:

```bibtex
@inproceedings{wang2025needleinatable,
  title={NEEDLEINATABLE: Exploring Long-Context Capability of Large Language Models towards Long-Structured Tables},
  author={Wang, Lanrui and Zheng, Mingyu and Tang, Hongyin and Lin, Zheng and Cao, Yanan and Wang, Jingang and Cai, Xunliang and Wang, Weiping},
  booktitle={39th Conference on Neural Information Processing Systems (NeurIPS 2025)},
  year={2025}
}
```

## 📧 Contact

For inquiries regarding the full dataset or the codebase, please contact:

*   **Lanrui Wang:** oliveerwang@tencent.com
*   **Mingyu Zheng:** zhengmingyu@iie.ac.cn
*   **Zheng Lin:** linzheng@iie.ac.cn

**Paper Link:** [arXiv:2504.06560](https://arxiv.org/abs/2504.06560)