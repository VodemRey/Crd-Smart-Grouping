CRD REC SMART GROUPING

Overview

CRD REC Smart Grouping is a standalone Python project based on the workflow of the CRD Rec Smart Grouping agent.
It is structured as a code-based implementation of the existing processing logic instead of keeping that logic only inside prompts.
The project serves as a technical implementation of the Smart Grouping workflow in a simpler and more transparent form.

Purpose
The purpose of this project is to automate the processing of reconciliation files and reduce manual work.
It is meant to improve grouping quality and make the logic easier to maintain by moving it from prompts into Python code.
It also creates a base that can be extended later without rebuilding the solution from scratch.

Data sources

* Item Status
* Crystal Open Items
* Cash Matrix
* Key Values (static reference)

Output
The project produces an Excel results file with one sheet per processed input file.
The output keeps the original data and adds new processing columns such as issuer assignment, key phrases, amount, and group number.
Grouped rows are prepared in the final workbook so they can be reviewed by the user and used for grouping.

Pipeline

1. Discover inputs

   * find tabular files
   * identify issuers file vs items files
   * detect profile (item_status / crystal_open_items / cash_matrix)

2. Load raw data

   * read file
   * detect sheet/header row
   * create raw DataFrame

3. Resolve schema

   * map source headers to canonical columns
   * keep only schema/header normalization here

4. Validate structure

   * check issuers file exists
   * check required issuer columns exist
   * check required canonical item columns exist
   * fail early if structure is invalid

5. Normalize content

   * clean text fields
   * build reference_text
   * normalize amount / entry_type / currency / segment

6. Assign issuer

   * apply issuer assignment rules
   * write assigned_issuer + explanation fields

7. Prepare grouping fields

   * create signed amount
   * mark reusable rows / grouping buckets
   * keep deterministic sort keys

8. Group Pass 1

   * group within assigned issuers
   * same currency + same segment/reconciliation
   * assign first Group Numbers

9. Classify A / B / C

   * A = assigned + grouped
   * B = assigned + not grouped
   * C = not assigned

10. Group Pass 2

    * combine B with eligible C
    * same currency + same segment/reconciliation
    * continue Group Numbers without row reuse

11. Export results

    * save workbook
    * keep original columns
    * append technical/debug/grouping columns

Project structure

```text
Crd-Smart-Grouping/
├─ data/
│  └─ input/
│     ├─ Entry.xlsx         │ input items data file
│     └─ Key values.xlsx    │ issuer keys reference file
│
├─ src/
│  ├─ main.py               │ orchestration only
│  ├─ config.py             │ settings, thresholds, required columns, output naming
│  ├─ profiles.py           │ input profile mappings
│  ├─ dataset_detector.py   │ detect issuers vs items files + dataset type
│  ├─ loader.py             │ read Excel/CSV, detect sheet/header
│  ├─ normalize.py          │ canonical mapping, text normalization, reference_text, amount prep
│  ├─ validators.py         │ input and schema validation
│  ├─ issuer_assigner.py    │ issuer assignment + match explanation
│  ├─ grouping.py           │ pass1, classify A/B/C, pass2 grouping
│  └─ exporter.py           │ save results and build output summary
│
├─ README.md                │ project description and usage
└─ requirements.txt         │ python dependencies
```

