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

## Technical requirements

The pipeline has been designed to run as a lightweight and modular QA system with minimal dependencies.

### Software requirements
- Python 3.9+
- pandas
- numpy

### Data requirements
- Input data must be provided as CSV files with a timestamp index
- Files must follow the naming convention:
  
```
StationName_dd_mm_yyyy.csv
```

- Data must be preprocessed into a structured format (one column per variable)

### Configuration
- Validation thresholds are defined in a central configuration file (`settings.py`)
- This allows easy adjustment without modifying the core logic

### Execution
- The pipeline is designed to run:
- daily (operational mode)
- manually for testing (custom reference date)

### Output
- The system generates:
- structured issue logs
- summaries per validation dimension
- execution-level reports for communication

### Design considerations
- Modular architecture (completeness, temporal, physical)
- Clear separation between validation and reporting
- Designed for scalability across multiple stations

## Data structure

The project follows a structured data workflow:

```
data/
├── raw/              # Raw input data (not tracked)
├── processed/        # Preprocessed station data (not tracked)
├── qa_reports/       # Generated QA outputs (not tracked)
└── sample/           # Example dataset for demonstration
    ├── input/
    └── output/
```

- The `raw`, `processed` and `qa_reports` folders are excluded from version control using `.gitignore`.
- The `sample` folder contains a small example dataset and corresponding outputs to illustrate the pipeline behavior.

## Usage

Run the pipeline:

```bash
python qa_pipeline.py
