from __future__ import annotations

from pathlib import Path
import logging
import uuid

import pandas as pd

from config.settings import (
    PROCESSED_DIR,
    QA_REPORTS_DIR,
    QA_LOGS_DIR,
    FILE_PATTERN,
    FILE_ENCODING,
    QA_COLUMN_MAP,
    QA_COLUMN_MAP_INV,
    TEMPORAL_THRESHOLDS,
    PHYSICAL_THRESHOLDS,
    ISSUE_REPORTING_RULES,
)
from src.qa.completeness import run_completeness_checks
from src.qa.temporal import run_temporal_checks
from src.qa.physical import run_physical_checks
from src.reporting.reporting import (
    enrich_issues_with_reporting_fields,
    build_execution_summary,
    build_station_overview,
    generate_execution_report_text,
    save_text_report,
)


# ============================================================
# Step 0. Logging and execution context
# ============================================================
def generate_execution_id() -> str:
    """
    Generate a short unique execution ID for the current QA run.
    """
    return str(uuid.uuid4())[:8]


def setup_logger(log_dir: Path, execution_id: str, run_datetime: pd.Timestamp) -> Path:
    """
    Configure logging to both console and file.
    """
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp_str = pd.to_datetime(run_datetime).strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"qa_pipeline_{timestamp_str}_{execution_id}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return log_file

# ============================================================
# Step 1. Processed file metadata extraction
# ============================================================

def extract_analysis_date_from_filename(file_path: Path) -> pd.Timestamp:
    """
    Extract analysis date from processed file name.
    Example: SanXoan_08_01_2026.csv -> 2026-01-08
    """
    stem = file_path.stem
    date_str = "_".join(stem.split("_")[-3:])
    return pd.to_datetime(date_str, format="%d_%m_%Y")


def extract_station_name_from_filename(file_path: Path) -> str:
    """
    Extract station name from processed file name.
    Example: SanXoan_08_01_2026.csv -> SanXoan
    """
    stem = file_path.stem
    return stem.rsplit("_", 3)[0]

def get_expected_analysis_date(reference_date: pd.Timestamp) -> pd.Timestamp:
    """
    Expected analysis date for daily QA execution: yesterday.
    """
    reference_date = pd.to_datetime(reference_date).normalize()
    return reference_date - pd.Timedelta(days=1)

# ============================================================
# Step 2. Processed data ingestion
# ============================================================

def read_processed_station_file(file_path: Path, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Read one processed station CSV with Fecha as datetime index.
    """
    df = pd.read_csv(
        file_path,
        sep=";",
        encoding=encoding,
        index_col="Fecha",
        parse_dates=True,
    )

    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()]
    df = df.sort_index()

    return df

# ============================================================
# Step 3. Pipeline-level validation
# ============================================================
def validate_file_freshness(
    station_name: str,
    file_name: str,
    analysis_date: pd.Timestamp,
    expected_analysis_date: pd.Timestamp,
) -> list[dict]:
    """
    Validate that processed file date matches expected analysis date.
    """
    issues: list[dict] = []

    if analysis_date.normalize() != expected_analysis_date.normalize():
        issues.append(
            {
                "station_name": station_name,
                "file_name": file_name,
                "check_dimension": "pipeline",
                "issue_type": "stale_processed_file",
                "variable": None,
                "timestamp": None,
                "timestamp_start": None,
                "timestamp_end": None,
                "duration_minutes": None,
                "detail": (
                    f"Processed file analysis date is {analysis_date.date()}, "
                    f"but expected analysis date is {expected_analysis_date.date()}."
                ),
            }
        )

    return issues

# ============================================================
# Step 4. QA column standardization
# ============================================================

def build_qa_check_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a copy of the processed dataframe with standardized column names
    for QA checks.
    """
    qa_df = df.copy()

    # Normalize column names to avoid hidden non-breaking spaces
    qa_df.columns = (
        qa_df.columns
        .str.replace("\xa0", " ", regex=False)
        .str.strip()
    )

    return qa_df.rename(columns=QA_COLUMN_MAP).copy()


def restore_original_variable_names(
    issues: list[dict],
) -> list[dict]:
    """
    Replace standardized variable names in issue records with original dataset names.
    """
    restored_issues = []

    for issue in issues:
        issue_copy = issue.copy()
        variable = issue_copy.get("variable")

        if variable in QA_COLUMN_MAP_INV:
            issue_copy["variable"] = QA_COLUMN_MAP_INV[variable]

        restored_issues.append(issue_copy)

    return restored_issues

# ============================================================
# Step 5. QA module execution
# ============================================================

def process_all_processed_files(
    processed_dir: Path,
    reference_date: pd.Timestamp,
    validate_freshness: bool = True,
    pattern: str = "*.csv",
    encoding: str = "utf-8",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run QA checks for all processed station files across all configured dimensions.
    """
    files = sorted(processed_dir.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No processed files found in {processed_dir} with pattern {pattern}")

    all_issues: list[dict] = []
    all_completeness_summaries: list[dict] = []
    all_temporal_summaries: list[dict] = []
    all_physical_summaries: list[dict] = []

    expected_analysis_date = get_expected_analysis_date(reference_date)

    for file_path in files:
        station_name = extract_station_name_from_filename(file_path)
        analysis_date = extract_analysis_date_from_filename(file_path)
        df = read_processed_station_file(file_path, encoding=encoding)
        qa_df = build_qa_check_dataframe(df)

        pipeline_issues: list[dict] = []
        if validate_freshness:
            pipeline_issues = validate_file_freshness(
                station_name=station_name,
                file_name=file_path.name,
                analysis_date=analysis_date,
                expected_analysis_date=expected_analysis_date,
            )

        completeness_issues, completeness_summary = run_completeness_checks(
            df=df,
            station_name=station_name,
            file_name=file_path.name,
            analysis_date=analysis_date,
            expected_frequency_minutes=10,
        )

        temporal_issues, temporal_summary = run_temporal_checks(
            df=qa_df,
            station_name=station_name,
            file_name=file_path.name,
            abrupt_thresholds=TEMPORAL_THRESHOLDS["abrupt"],
            spike_thresholds=TEMPORAL_THRESHOLDS["spike"],
            persistence_thresholds=TEMPORAL_THRESHOLDS["persistence"],
            exclude_zero_columns=TEMPORAL_THRESHOLDS["exclude_zero_columns"],
        )
        
        temporal_issues = restore_original_variable_names(temporal_issues)
        
        physical_issues, physical_summary = run_physical_checks(
            df=qa_df,
            station_name=station_name,
            file_name=file_path.name,
            physical_range_thresholds=PHYSICAL_THRESHOLDS["ranges"],
            rain_low_humidity_thresholds=PHYSICAL_THRESHOLDS["rain_low_humidity"],
        )
        
        physical_issues = restore_original_variable_names(physical_issues)


        all_issues.extend(pipeline_issues)
        all_issues.extend(completeness_issues)
        all_issues.extend(temporal_issues)
        all_issues.extend(physical_issues)
        
        completeness_summary["analysis_date"] = analysis_date.date()
        completeness_summary["pipeline_issue_count"] = len(pipeline_issues)
        completeness_summary["total_station_issues"] = len(pipeline_issues) + completeness_summary["total_completeness_issues"]
        completeness_summary["status"] = "OK" if completeness_summary["total_station_issues"] == 0  else "WARNING"
        
        temporal_summary["analysis_date"] = analysis_date.date()
        temporal_summary["pipeline_issue_count"] = len(pipeline_issues)
        temporal_summary["total_station_issues"] = len(pipeline_issues) + temporal_summary["total_temporal_issues"]
        temporal_summary["status"] = "OK" if temporal_summary["total_station_issues"] == 0  else "WARNING"
        
        physical_summary["analysis_date"] = analysis_date.date()
        physical_summary["pipeline_issue_count"] = len(pipeline_issues)
        physical_summary["total_station_issues"] = len(pipeline_issues) + physical_summary["total_physical_issues"]
        physical_summary["status"] = "OK" if physical_summary["total_station_issues"] == 0  else "WARNING"

        all_completeness_summaries.append(completeness_summary)
        all_temporal_summaries.append(temporal_summary)
        all_physical_summaries.append(physical_summary)

        logging.info(
            f"station={station_name} | file={file_path.name} | analysis_date={analysis_date.date()} | "
            f"rows={len(df)} | completeness_issues={completeness_summary['total_completeness_issues']} | "
            f"temporal_issues={temporal_summary['total_temporal_issues']} | "
            f"physical_issues={physical_summary['total_physical_issues']} | "
            f"pipeline_issues={len(pipeline_issues)}"
        )

    issues_df = pd.DataFrame(all_issues)
    summary_completeness_df = pd.DataFrame(all_completeness_summaries)
    summary_temporal_df = pd.DataFrame(all_temporal_summaries)
    summary_physical_df = pd.DataFrame(all_physical_summaries)

    return issues_df, summary_completeness_df, summary_temporal_df, summary_physical_df

# ============================================================
# Step 6.Output path generation and persistence
# ============================================================
def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save dataframe to CSV.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")


def build_output_paths(
    reports_dir: Path,
    execution_id: str,
    run_datetime: pd.Timestamp,
) -> tuple[Path, Path, Path, Path, Path, Path, Path]:
    """
    Build output paths for issues and dimension summaries.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)

    timestamp_str = pd.to_datetime(run_datetime).strftime("%Y%m%d_%H%M%S")
    issues_path = reports_dir / f"issues_{timestamp_str}_{execution_id}.csv"
    completeness_summary_path = reports_dir / f"completeness_summary_{timestamp_str}_{execution_id}.csv"
    temporal_summary_path = reports_dir / f"temporal_summary_{timestamp_str}_{execution_id}.csv"
    physical_summary_path = reports_dir / f"physical_summary_{timestamp_str}_{execution_id}.csv"
    execution_summary_path = reports_dir / f"execution_summary_{timestamp_str}_{execution_id}.csv"
    station_overview_path = reports_dir / f"station_overview_{timestamp_str}_{execution_id}.csv"
    execution_report_path = reports_dir / f"execution_report_{timestamp_str}_{execution_id}.txt"

    return issues_path, completeness_summary_path, temporal_summary_path, physical_summary_path, execution_summary_path, station_overview_path, execution_report_path


# ============================================================
# Step 7.Main QA pipeline runner
# ============================================================

def run_qa_pipeline(
    reference_date: pd.Timestamp | None = None,
    validate_freshness: bool = True,
) -> None:
    """
    Main QA pipeline entry point.
    """
    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()

    execution_id = generate_execution_id()
    log_file = setup_logger(
        log_dir=QA_LOGS_DIR,
        execution_id=execution_id,
        run_datetime=reference_date,
    )

    logging.info(f"[{execution_id}] Starting QA pipeline")
    logging.info(f"[{execution_id}] Log file: {log_file}")
    logging.info(f"[{execution_id}] Processed directory: {PROCESSED_DIR}")
    logging.info(f"[{execution_id}] Reports directory: {QA_REPORTS_DIR}")
    logging.info(f"[{execution_id}] Reference date: {reference_date}")
    logging.info(f"[{execution_id}] Validate freshness: {validate_freshness}")

    issues_df, completeness_summary_df, temporal_summary_df, physical_summary_df = process_all_processed_files(
    processed_dir=PROCESSED_DIR,
    reference_date=reference_date,
    validate_freshness=validate_freshness,
    pattern=FILE_PATTERN,
    encoding=FILE_ENCODING,
)
    
     # ----------------------------------------------------
    # Reporting enrichment
    # ----------------------------------------------------
    issues_df = enrich_issues_with_reporting_fields(
        issues_df=issues_df,
        rules=ISSUE_REPORTING_RULES,
    )

    execution_summary_df = build_execution_summary(
        issues_df=issues_df,
        completeness_summary_df=completeness_summary_df,
        temporal_summary_df=temporal_summary_df,
        physical_summary_df=physical_summary_df,
    )

    station_overview_df = build_station_overview(issues_df)

    report_text = generate_execution_report_text(
        execution_id=execution_id,
        reference_date=reference_date,
        execution_summary_df=execution_summary_df,
        station_overview_df=station_overview_df,
    )

    # ----------------------------------------------------
    # Output paths
    # ----------------------------------------------------

    issues_path, completeness_summary_path, temporal_summary_path, physical_summary_path, execution_summary_path, station_overview_path, execution_report_path = build_output_paths(
        reports_dir=QA_REPORTS_DIR,
        execution_id=execution_id,
        run_datetime=reference_date,
    )

    # ----------------------------------------------------
    # Save outputs
    # ----------------------------------------------------
    save_dataframe(issues_df, issues_path)
    save_dataframe(completeness_summary_df, completeness_summary_path)
    save_dataframe(temporal_summary_df, temporal_summary_path)
    save_dataframe(physical_summary_df, physical_summary_path)
    save_dataframe(execution_summary_df, execution_summary_path)
    save_dataframe(station_overview_df, station_overview_path)
    save_text_report(report_text, execution_report_path)

    # ----------------------------------------------------
    # Final logging
    # ----------------------------------------------------    

    logging.info(f"[{execution_id}] Issues report saved: {issues_path}")
    logging.info(f"[{execution_id}] Completeness summary report saved: {completeness_summary_path}")
    logging.info(f"[{execution_id}] Temporal summary report saved: {temporal_summary_path}")
    logging.info(f"[{execution_id}] Physical summary report saved: {physical_summary_path}")
    logging.info(f"[{execution_id}] Execution summary report saved: {execution_summary_path}")
    logging.info(f"[{execution_id}] Station overview report saved: {station_overview_path}")
    logging.info(f"[{execution_id}] Execution text report saved: {execution_report_path}")
    logging.info(
        f"[{execution_id}] QA pipeline finished | "
        f"stations={execution_summary_df.iloc[0]['stations_processed'] if not execution_summary_df.empty else 0} | "
        f"issues={len(issues_df)} | "
        f"overall_status={execution_summary_df.iloc[0]['status'] if not execution_summary_df.empty else 'UNKNOWN'}"
    )


# ============================================================
# Step 8.Execution entry point
# ============================================================

if __name__ == "__main__":
    # Daily execution
    REFERENCE_DATE = pd.Timestamp.now().normalize()
    VALIDATE_FRESHNESS = True

    # For testing:
    # REFERENCE_DATE = pd.Timestamp("2026-01-09").normalize()
    # VALIDATE_FRESHNESS = False

    run_qa_pipeline(
        reference_date=REFERENCE_DATE,
        validate_freshness=VALIDATE_FRESHNESS,
    )
