# Money in Motion: Micro‑Velocity and Usage of Ethereum’s Liquid Staking Tokens (stETH / wstETH)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](#prerequisites)
[![arXiv](https://img.shields.io/badge/arXiv-2508.15391-B31B1B)](https://arxiv.org/abs/2508.15391)
[![Data: OSF](https://img.shields.io/badge/Data-OSF-0077B5)](https://osf.io/2mskb/)


Reproducible **code & data** for the paper:

> **Money in Motion: Micro‑Velocity and Usage of Ethereum’s Liquid Staking Tokens**
> Benjamin Kraner, Luca Pennella, Nicolò Vallarano, Claudio J. Tessone (2025)
> arXiv: 2508.15391 — https://arxiv.org/abs/2508.15391

This repository provides a fully reproducible pipeline to compute **address‑level micro‑velocity** and balance dynamics for Lido’s **stETH (rebasing)** and **wstETH (non‑rebasing)**. It includes:

* Two **open‑source tools** to collect on‑chain data (event logs & historical contract state),
* A **curated dataset** (ready-to-use) with all relevant events, hosted on **OSF**,
* Scripts to **run the end‑to‑end pipeline** from raw Ethereum data.

> **Data coverage**: events and state snapshots up to **2024‑11‑08** (see the paper for details).

---

## Contents

* [Prerequisites](#prerequisites)
* [Quickstart](#quickstart)
* [Environment variables](#environment-variables)
* [Repository structure](#repository-structure)
* [Outputs](#outputs)
* [User categories](#user-categories)
* [Data availability](#data-availability)
* [Citing](#citing)
* [License](#license)
* [Troubleshooting](#troubleshooting)

---

## Prerequisites

* **Python 3.10+** (tested on 3.10/3.11)
* **conda** (recommended) or `venv`
* Access to an **Ethereum RPC endpoint** (HTTPS; archive or at least log‑capable provider)
* Optional: Etherscan (or equivalent) API key for log retrieval, depending on your provider


### External tools used by this repo

Install the following packages in **editable** mode (used by the scripts here):

```bash
# 1) Event logs indexer
git clone https://gitlab.uzh.ch/bdlt/ethereum-event-tracker.git
pip install -e ethereum-event-tracker/

# 2) Historical state tracker
git clone https://gitlab.uzh.ch/bdlt/ethereum-variable-tracker.git
pip install -e ethereum-variable-tracker/

# 3) Micro‑velocity engine (parallelised branch)
git clone -b parallelised_plus_bilance https://github.com/fdecollibus/MicroVelocityAnalyzer.git
pip install -e MicroVelocityAnalyzer/
```

---

## Run the full pipeline (from raw data)

1. **Event logs** :

```bash
# stETH (Transfer + TransferShares)
bash script/01_stETH_event_collection.sh

# wstETH (Transfer)
bash script/03_wstETH_event_collection.sh
```

2. **Lido state variables** (stETH only):

```bash
bash script/02_stETH_variable_collection.sh
```

3. **Preprocess & compute micro‑velocity**:

```bash
# stETH
bash script/05_stETH_microvelocity.sh

# wstETH
bash script/07_wstETH-microvelocity.sh
```

4. **Post‑process results**:

```bash
# stETH
bash script/09_stETH_postprocess.sh

# wstETH
bash script/11_wstETH_postprocess.sh
```

---
## Quickstart
### Scripts reference (what each script does)

> Complete index of the scripts under `script/` with role, main inputs, and outputs.

**1) Raw data collection**

* `01_stETH_event_collection.sh`
  **Purpose:** downloads `Transfer` and `TransferShares` events for **stETH** in 50k‑block shards.
  **Reads from:** `.env` (RPC URL, `START_BLOCK`, `END_BLOCK`, `SHARD_SIZE`, API keys).
  **Writes to:** `input/event/` (one file per event per range).

* `02_stETH_variable_collection.sh`
  **Purpose:** collects Lido variable snapshots every 50k blocks (e.g., `BUFFERED_ETHER_POSITION`, `BEACON_BALANCE_POSITION`, `DEPOSITED_VALIDATORS_POSITION`, `BEACON_VALIDATORS_POSITION`, `TOTAL_SHARES_POSITION`).
  **Writes to:** `input/variables/`.

* `03_wstETH_event_collection.sh`
  **Purpose:** downloads `Transfer` events for **wstETH** in 50k‑block shards.
  **Writes to:** `input/event/`.

**2) Preprocess for MicroVelocity**

* `04_stETH_preprocess_MicroVelocity.py`
  **Purpose:** prepares **stETH** files (shares allocations and transfers) in the format required by `MicroVelocityAnalyzer`.
  **Output:** `$PROCESSED/shares-allocated.csv`, `$PROCESSED/shares-transfers.csv`, `$PROCESSED/stETH-TransferShares.parquet`, `$PROCESSED/lido-variables.parquet`.

* `06_wstETH_preprocess_MicroVelocity.py`
  **Purpose:** prepares **wstETH** files (token transfers) in the required format.

**3) Micro‑velocity computation**

* `05_stETH_microvelocity.sh`
  **Purpose:** runs `MicroVelocityAnalyzer` on **stETH** (branch `parallelised_plus_bilance`).
  **Output:** pickle with per‑address balances & velocities sampled every `N_BLOCKS`.

* `07_wstETH-microvelocity.sh`
  **Purpose:** runs `MicroVelocityAnalyzer` on **wstETH**.

**4) Post‑process & aggregations**

* `08_stETH_postprocess_MicroVelocity.py`
  **Purpose:** final cleaning and transformations on **stETH** to obtain tidy records.

* `09_stETH_postprocess.sh`
  **Purpose:** aggregates **stETH** by **user categories**;
  **Output:** records/dicts with keys `timestamp`, `blocknumber`, `PQ_total`, `V_[cat]`, `M_[cat]`, `MV_[cat]`.

* `10_wstETH_postprocess_MicroVelocity.py`
  **Purpose:** final cleaning and transformations on **wstETH**.

* `11_wstETH_postprocess.sh`
  **Purpose:** aggregates **wstETH** by **user categories**.




---

## Repository structure

```
.
├── abi/                    # ABIs used by collectors
├── event/                  # Helpers for event collection
├── input/
│   ├── event/              # Raw event shards (50k‑block buckets)
│   └── variables/          # Raw variable snapshots
├── output/                 # Final artefacts (pickles, csv, parquet)
├── script/                 # Shell scripts to run the pipeline
├── Dataset/                # Curated dataset for quick reproduction
└── README.md
```

---


## User categories

Used to aggregate activity by wealth tiers:

* `Whale` (≥ 10k stETH)
* `Orca` (3–10k)
* `Dolphin` (1–3k)
* `Fish` (100–1k)
* `Shrimp` (10–100)
* `Krill` (1–10)
* `Plankton` (< 1)
* `high` (≥ 100 stETH; Whale→Fish)
* `low` (< 100; Shrimp→Plankton)
* `total` (all)

---
## Data availability

The curated dataset is hosted on **OSF** for stable, citable access and better handling of large files.  
https://osf.io/2mskb/

---

## Citing

If you use this code or dataset, please cite the paper.

**Paper**

```bibtex
@article{kraner2025moneyinmotion,
  title   = {Money in Motion: Micro-Velocity and Usage of Ethereum’s Liquid Staking Tokens},
  author  = {Kraner, Benjamin and Pennella, Luca and Vallarano, Nicolò and Tessone, Claudio J.},
  year    = {2025},
  eprint  = {2508.15391},
  archivePrefix = {arXiv},
  primaryClass = {q-fin.GN},
  doi     = {10.48550/arXiv.2508.15391}
}
```

---

**Questions or issues?** Feel free to open an issue or reach out to the authors.
