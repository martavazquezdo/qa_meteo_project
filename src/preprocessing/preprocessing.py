from pathlib import Path
import logging
import unicodedata
import uuid
import pandas as pd


# ============================================================
# Step 0. Logging, execution context and issue tracking
# ============================================================

def generate_execution_id() -> str:
    """
    Generate a short unique execution ID for the current pipeline run.
    """
    return str(uuid.uuid4())[:8]


def setup_logger(log_dir: str, execution_id: str, run_datetime: pd.Timestamp) -> Path:
    """
    Configure logging to both console and file.

    Returns the log file path.
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    timestamp_str = pd.to_datetime(run_datetime).strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"preprocessing_{timestamp_str}_{execution_id}.log"

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


def create_issue_record(
    execution_id: str,
    file_name: str,
    station_name: str,
    issue_type: str,
    issue_detail: str
) -> dict:
    """
    Create a standardized issue record.
    """
    return {
        "execution_id": execution_id,
        "file_name": file_name,
        "station_name": station_name,
        "issue_type": issue_type,
        "issue_detail": issue_detail
    }


def save_issues_log(
    issues: list[dict],
    log_dir: str,
    execution_id: str,
    run_datetime: pd.Timestamp
) -> Path | None:
    """
    Save preprocessing issues to CSV if any exist.
    """
    if not issues:
        return None

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    timestamp_str = pd.to_datetime(run_datetime).strftime("%Y%m%d_%H%M%S")
    issues_file = log_path / f"preprocessing_issues_{timestamp_str}_{execution_id}.csv"

    df_issues = pd.DataFrame(issues)
    df_issues.to_csv(issues_file, sep=";", index=False, encoding="utf-8-sig")

    return issues_file


# ============================================================
# Step 1. Metadata utilities
# ============================================================

def clean_station_name(text: str) -> str:
    """
    Clean station name for safe and consistent file naming.
    """
    text = str(text).strip().strip('"')
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = text.replace(" ", "")
    return text


def extract_station_name(file_path: Path, encoding: str = "utf-8") -> str:
    """
    Extract station name from the second line of the raw CSV file.
    Assumes only one non-empty cell contains the station name.
    """
    with file_path.open("r", encoding=encoding) as f:
        _ = f.readline().strip()
        second_line = f.readline().strip()

    parts = second_line.split(";")
    valid_cells = [x.strip().strip('"') for x in parts if x.strip().strip('"')]

    if not valid_cells:
        raise ValueError(f"Station name could not be extracted from {file_path.name}")

    return clean_station_name(valid_cells[0])


# ============================================================
# Step 2. Raw data ingestion
# ============================================================

def read_raw_station_csv(file_path: Path, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Read raw station CSV, skipping metadata rows.
    """
    return pd.read_csv(file_path, sep=";", header=2, encoding=encoding)


# ============================================================
# Step 3. Data transformation
# ============================================================

def build_variable_header(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a combined variable header using variable name and unit.
    """
    required_cols = {"Variable", "Unidad", "Valor", "Fecha"}
    missing_cols = required_cols - set(df.columns)

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df.copy()
    df["variable_header"] = (
        df["Variable"].astype(str).str.strip() +
        "(" +
        df["Unidad"].astype(str).str.strip() +
        ")"
    )
    return df


def pivot_station_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform long-format data into a wide-format table with Fecha as index.
    """
    df_pivot = pd.pivot_table(
        df,
        values="Valor",
        index="Fecha",
        columns="variable_header",
        aggfunc="first"
    )

    df_pivot.columns.name = None
    df_pivot.index = pd.to_datetime(df_pivot.index, dayfirst=True, errors="coerce")
    df_pivot = df_pivot[~df_pivot.index.isna()]
    df_pivot = df_pivot.sort_index()
    df_pivot.index.name = "Fecha"

    return df_pivot


# ============================================================
# Step 4. Daily execution window logic
# ============================================================

def get_expected_dates(run_datetime: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Calculate:
    - analysis start: D-2 at 18:00
    - expected central day: D-1
    """
    run_datetime = pd.to_datetime(run_datetime)
    run_day = run_datetime.normalize()

    start = (run_day - pd.Timedelta(days=2)) + pd.Timedelta(hours=18)
    central_day = run_day - pd.Timedelta(days=1)

    return start, central_day


def validate_expected_days(df: pd.DataFrame, run_datetime: pd.Timestamp) -> None:
    """
    Validate that the dataset contains the three expected calendar days:
    D-2, D-1 and D.
    """
    run_day = pd.to_datetime(run_datetime).normalize()

    expected_days = {
        (run_day - pd.Timedelta(days=2)).date(),
        (run_day - pd.Timedelta(days=1)).date(),
        run_day.date(),
    }

    available_days = set(df.index.date)
    missing_days = expected_days - available_days

    if missing_days:
        raise ValueError(f"Missing expected day(s): {sorted(missing_days)}")


def validate_expected_start(df: pd.DataFrame, expected_start: pd.Timestamp) -> None:
    """
    Validate that data reaches at least the expected analysis start.
    """
    if df.index.max() < expected_start:
        raise ValueError(
            f"Dataset does not reach expected analysis start: {expected_start}"
        )

    if not (df.index >= expected_start).any():
        raise ValueError(
            f"No records found from expected analysis start onward: {expected_start}"
        )


def get_last_available_timestamp_for_run_day(
    df: pd.DataFrame,
    run_datetime: pd.Timestamp
) -> pd.Timestamp:
    """
    Return the last available timestamp belonging to the execution day.
    """
    run_day = pd.to_datetime(run_datetime).normalize()
    next_day = run_day + pd.Timedelta(days=1)

    df_run_day = df[(df.index >= run_day) & (df.index < next_day)]

    if df_run_day.empty:
        raise ValueError("No data available for execution day.")

    return df_run_day.index.max()


def filter_dynamic_analysis_window(
    df: pd.DataFrame,
    run_datetime: pd.Timestamp
) -> tuple[pd.DataFrame, pd.Timestamp]:
    """
    Build dynamic daily analysis window:
    - start: D-2 at 18:00
    - end: last available record on run day
    """
    expected_start, expected_central_day = get_expected_dates(run_datetime)

    validate_expected_days(df, run_datetime)
    validate_expected_start(df, expected_start)

    dynamic_end = get_last_available_timestamp_for_run_day(df, run_datetime)

    filtered_df = df[(df.index >= expected_start) & (df.index <= dynamic_end)].copy()
    filtered_df = filtered_df.sort_index()

    if filtered_df.empty:
        raise ValueError("Filtered dataframe is empty after applying daily analysis window.")

    return filtered_df, expected_central_day


def infer_central_day(df: pd.DataFrame) -> pd.Timestamp:
    """
    Infer central day as the day with the highest number of records.
    """
    if df.empty:
        raise ValueError("Filtered dataframe is empty; central day cannot be inferred.")

    counts = pd.Series(df.index.date).value_counts()
    central_day = counts.idxmax()

    return pd.to_datetime(central_day)


# ============================================================
# Step 5. Output naming and storage
# ============================================================

def build_output_filename(station_name: str, central_day: pd.Timestamp) -> str:
    """
    Build output file name using station name and central day.
    """
    date_str = central_day.strftime("%d_%m_%Y")
    return f"{station_name}_{date_str}.csv"


def save_processed_data(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save processed dataframe to CSV preserving Fecha as index.
    """
    df.to_csv(output_path, sep=";", encoding="utf-8-sig", index=True)


# ============================================================
# Step 6. Single-file preprocessing pipeline
# ============================================================

def preprocess_station_file(
    file_path: Path,
    output_dir: Path,
    run_datetime: pd.Timestamp,
    execution_id: str,
    issues: list[dict],
    encoding: str = "utf-8"
) -> Path:
    """
    Full preprocessing pipeline for a single station file.
    """
    station_name = "UNKNOWN"

    try:
        # 1. Extract station metadata
        station_name = extract_station_name(file_path, encoding=encoding)

        # 2. Read raw data
        raw_df = read_raw_station_csv(file_path, encoding=encoding)
        raw_rows = len(raw_df)

        # 3. Transform raw data into analysis-ready format
        raw_df = build_variable_header(raw_df)
        pivot_df = pivot_station_data(raw_df)

        # 4. Apply daily temporal window
        filtered_df, expected_central_day = filter_dynamic_analysis_window(
            pivot_df,
            run_datetime=run_datetime
        )

        # 5. Validate inferred central day against expected central day
        inferred_central_day = infer_central_day(filtered_df)
        status = "OK"

        if inferred_central_day.normalize() != expected_central_day.normalize():
            status = "WARNING"
            issues.append(
                create_issue_record(
                    execution_id=execution_id,
                    file_name=file_path.name,
                    station_name=station_name,
                    issue_type="central_day_mismatch",
                    issue_detail=(
                        f"Expected central day {expected_central_day.date()} "
                        f"but inferred {inferred_central_day.date()}"
                    )
                )
            )

        # 6. Save processed dataset
        output_dir.mkdir(parents=True, exist_ok=True)
        output_name = build_output_filename(station_name, expected_central_day)
        output_path = output_dir / output_name
        save_processed_data(filtered_df, output_path)

        # Compact operational log
        logging.info(
            f"[{execution_id}] station={station_name} | file={file_path.name} | "
            f"raw_rows={raw_rows} | filtered_rows={len(filtered_df)} | "
            f"start={filtered_df.index.min()} | end={filtered_df.index.max()} | "
            f"status={status}"
        )

        return output_path

    except Exception as exc:
        issues.append(
            create_issue_record(
                execution_id=execution_id,
                file_name=file_path.name,
                station_name=station_name,
                issue_type="preprocessing_error",
                issue_detail=str(exc)
            )
        )

        logging.error(
            f"[{execution_id}] station={station_name} | file={file_path.name} | "
            f"status=ERROR | reason={exc}"
        )
        raise


# ============================================================
# Step 7. Batch pipeline for all files in folder
# ============================================================

def process_all_files(
    input_dir: str,
    output_dir: str,
    log_dir: str,
    run_datetime: pd.Timestamp | None = None,
    pattern: str = "*.csv",
    encoding: str = "utf-8"
) -> None:
    """
    Run preprocessing pipeline for all station files in input folder.
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if run_datetime is None:
        run_datetime = pd.Timestamp.now()

    execution_id = generate_execution_id()
    log_file = setup_logger(log_dir=log_dir, execution_id=execution_id, run_datetime=run_datetime)

    files = sorted(input_path.glob(pattern))
    issues: list[dict] = []

    logging.info(f"[{execution_id}] Starting preprocessing pipeline")
    logging.info(f"[{execution_id}] Log file: {log_file}")
    logging.info(f"[{execution_id}] Input directory: {input_path}")
    logging.info(f"[{execution_id}] Output directory: {output_path}")
    logging.info(f"[{execution_id}] Run datetime: {run_datetime}")

    expected_start, expected_central_day = get_expected_dates(run_datetime)
    logging.info(
        f"[{execution_id}] expected_start={expected_start} | expected_central_day={expected_central_day.date()}"
    )

    if not files:
        logging.warning(f"[{execution_id}] No files found in {input_path} with pattern {pattern}")
        return

    logging.info(f"[{execution_id}] files_found={len(files)}")

    processed_count = 0
    failed_count = 0

    for file_path in files:
        try:
            preprocess_station_file(
                file_path=file_path,
                output_dir=output_path,
                run_datetime=run_datetime,
                execution_id=execution_id,
                issues=issues,
                encoding=encoding
            )
            processed_count += 1
        except Exception:
            failed_count += 1

    issues_file = save_issues_log(
        issues=issues,
        log_dir=log_dir,
        execution_id=execution_id,
        run_datetime=run_datetime
    )

    logging.info(
        f"[{execution_id}] Pipeline finished | processed={processed_count} | failed={failed_count} | "
        f"issues={len(issues)} | issues_file={issues_file if issues_file else 'None'}"
    )


# ============================================================
# Step 8. Execution
# ============================================================

if __name__ == "__main__":
    
    from config.settings import (
    RAW_DIR,
    PROCESSED_DIR,
    PREPROCESSING_LOGS_DIR,
    FILE_PATTERN,
    FILE_ENCODING,
)

    # Production daily execution
    RUN_DATETIME = pd.Timestamp.now()
    

    # For testing:
    # RUN_DATETIME = pd.Timestamp("2026-01-09 10:00")

    process_all_files(
        input_dir=RAW_DIR,
        output_dir=PROCESSED_DIR,
        log_dir=PREPROCESSING_LOGS_DIR,
        run_datetime=RUN_DATETIME,
        pattern=FILE_PATTERN,
        encoding=FILE_ENCODING
    )