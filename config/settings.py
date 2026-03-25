from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
QA_REPORTS_DIR = DATA_DIR / "qa_reports"

LOGS_DIR = PROJECT_ROOT / "logs"
PREPROCESSING_LOGS_DIR = LOGS_DIR / "preprocessing"
QA_LOGS_DIR = LOGS_DIR / "qa"

FILE_PATTERN = "*.csv"
FILE_ENCODING = "utf-8"

RUN_HOUR = 10
ANALYSIS_START_OFFSET_DAYS = 2
ANALYSIS_START_HOUR = 18

# QA column standardization
QA_COLUMN_MAP = {
    "Temperatura  a 1.5m(ºC)": "temp_c",
    "Humidade relativa media a 1.5m(%)": "rh_pct",
    "Chuvia(L/m2)": "rain_l_m2",
}

QA_COLUMN_MAP_INV = {v: k for k, v in QA_COLUMN_MAP.items()}

TEMPORAL_THRESHOLDS = {

    # ----------------------------------------
    # TYPE OF CHANGE CALCULATION
    # ----------------------------------------
    "change_mode": {
        "temp_c": "absolute",      # °C
        "rh_pct": "absolute",      # %
        "rain_l_m2": "absolute",   # mm
    },

    # ----------------------------------------
    # ABRUPT CHANGES (Δ between steps)
    # ----------------------------------------
    "abrupt": {
        "temp_c": 5,
        "rh_pct": 20,
    },

    # ----------------------------------------
    # SPIKES (Δ up + Δ down)
    # ----------------------------------------
    "spike": {
        "temp_c": 6,
        "rh_pct": 25,
    },

    # ----------------------------------------
    # CONSTANT VALUE PERSISTENCE
    # ----------------------------------------
    "persistence": {
        "temp_c": 6,
        "rh_pct": 18,
        "rain_l_m2": 12,
    },

    # ----------------------------------------
    # EXCLUSIONS
    # ----------------------------------------
    "exclude_zero_columns": ["rain_l_m2"],
}

PHYSICAL_THRESHOLDS = {
    "ranges": {
        "temp_c": {"min": -50, "max": 50},
        "rh_pct": {"min": 0, "max": 100},
        "rain_l_m2": {"min": 0, "max": None},
    },
    "rain_low_humidity": {
        "rain_min": 0.1,
        "rh_min": 60,
    }
}

ISSUE_REPORTING_RULES = {
    "stale_processed_file": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "missing_expected_day": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "missing_analysis_start": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "missing_next_day_data": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "duplicated_timestamp": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "physical_range_low": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "physical_range_high": {
        "severity": "HIGH",
        "recommended_action": "review_required",
    },
    "temporal_gap": {
        "severity": "MEDIUM",
        "recommended_action": "review_required",
    },
    "missing_value": {
        "severity": "MEDIUM",
        "recommended_action": "review_required",
    },
    "abrupt_change": {
        "severity": "MEDIUM",
        "recommended_action": "monitor",
    },
    "constant_value_persistence": {
        "severity": "MEDIUM",
        "recommended_action": "monitor",
    },
    "rain_low_humidity_inconsistency": {
        "severity": "MEDIUM",
        "recommended_action": "review_required",
    },
    "isolated_spike": {
        "severity": "LOW",
        "recommended_action": "monitor",
    },
}