# Meteorological Data QA Pipeline

A modular Python pipeline for validating meteorological station data, designed to ensure the quality of inputs used in environmental and fire risk modeling.

---

## Objective

Detect inconsistencies in meteorological data and transform them into structured outputs that support:

* issue prioritization
* decision-making
* clear communication of results

---

## Validation Approach

The pipeline applies validation checks across three key dimensions:

### 1. Completeness

* missing values
* duplicated timestamps
* temporal gaps
* expected time coverage

### 2. Temporal Consistency

* abrupt changes
* constant value persistence (sensor blocking)
* isolated spikes

### 3. Physical Consistency

* physical range validation (e.g. RH > 100%, negative precipitation)
* cross-variable consistency (e.g. precipitation with low humidity)

---

## Outputs

The system generates:

* `issues.csv` → detected issues
* `*_summary.csv` → summaries by validation dimension
* `execution_summary.csv` → overall QA status
* `station_overview.csv` → station-level prioritization
* `execution_report.txt` → human-readable report

Each issue includes:

* severity level (HIGH, MEDIUM, LOW)
* recommended action

---

## System Design

* modular architecture (completeness, temporal, physical)
* clear separation between validation and reporting
* configurable thresholds (`settings.py`)
* scalable across multiple stations

---

## Data Structure

```text
data/
├── raw/            # raw input data (not tracked)
├── processed/      # preprocessed data (not tracked)
├── qa_reports/     # generated outputs (not tracked)
└── sample/         # example dataset
```

---

## Execution

```bash
python qa_pipeline.py
```

Linux:

```bash
bash run_pipeline.sh
```

---

## Testing & CI

The project includes:

* unit tests with controlled datasets
* automated execution via GitHub Actions

This ensures validation logic behaves as expected and prevents regressions.

---

## Context

This pipeline reflects a practical QA approach for environmental data, combining structured validation checks with outputs designed for operational use.

