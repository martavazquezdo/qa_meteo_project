# Meteorological Data QA Pipeline

Small QA pipeline for validating meteorological station data before using it as input for environmental or fire behavior models.

## Objective

Ensure data quality through structured validation checks and transform raw validation outputs into actionable insights for decision-making and communication.

## Pipeline Overview

The pipeline processes station data and applies validation checks across three dimensions:

### 1. Completeness
- Missing values
- Duplicated timestamps
- Temporal gaps
- Expected time window coverage

### 2. Temporal Consistency
- Abrupt changes
- Constant value persistence (sensor blocking)
- Isolated spikes

### 3. Physical Consistency
- Physical range validation (e.g. RH > 100%, negative precipitation)
- Cross-variable validation (e.g. precipitation with low humidity)

## Outputs

The pipeline generates:

- `issues.csv` → detailed list of detected issues
- `*_summary.csv` → summaries by validation dimension
- `execution_summary.csv` → global QA status
- `station_overview.csv` → station-level prioritization
- `execution_report.txt` → human-readable report

## Issue Classification

Each issue is enriched with:
- Severity (`HIGH`, `MEDIUM`, `LOW`)
- Recommended action (`review_required`, `monitor`)

This allows prioritization and facilitates communication of QA results.

## Usage

Run the pipeline:

```bash
python qa_pipeline.py
