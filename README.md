# Reconciliation Processing Tool

A Python-based data processing pipeline designed to prepare transaction datasets, assign issuers based on key phrases, and automatically detect reconciliation groups through deterministic matching rules.

The tool processes transaction entry files and matches rows using reference text and amount logic, producing structured reconciliation results.

---

# Overview

The system performs automated reconciliation in three main stages:

1. **Data preparation**
2. **Issuer assignment**
3. **Transaction grouping**

The goal is to transform raw transaction data into structured reconciliation groups with minimal manual intervention.

The program supports multiple entry datasets in a single run and produces a consolidated result file.

---
