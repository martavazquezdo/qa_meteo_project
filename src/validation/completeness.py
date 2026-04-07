from __future__ import annotations

import pandas as pd

# ============================================================
# Step 0. Issue record utilities
# ============================================================
def create_issue_record(
    station_name: str,
    file_name: str,
    issue_type: str,
    detail: str,
    variable: str | None = None,
    timestamp: pd.Timestamp | None = None,
    timestamp_start: pd.Timestamp | None = None,
    timestamp_end: pd.Timestamp | None = None,
    missing_count: int | None = None,
) -> dict:
    """
    Build a standardized completeness issue record.
    """
    return {
        "station_name": station_name,
        "file_name": file_name,
        "check_dimension": "completeness",
        "issue_type": issue_type,
        "variable": variable,
        "timestamp": timestamp,
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
        "missing_count": missing_count,
        "detail": detail,
    }

# ============================================================
# Step 1. Basic completeness checks
# ============================================================

def check_duplicated_timestamps(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
) -> list[dict]:
    """
    Detect duplicated timestamps in the dataframe index.
    """
    issues: list[dict] = []

    duplicated_mask = df.index.duplicated(keep=False)
    duplicated_index = df.index[duplicated_mask]

    if len(duplicated_index) == 0:
        return issues

    duplicated_counts = pd.Series(duplicated_index).value_counts().sort_index()

    for timestamp, count in duplicated_counts.items():
        issues.append(
            create_issue_record(
                station_name=station_name,
                file_name=file_name,
                issue_type="duplicated_timestamp",
                timestamp=timestamp,
                detail=f"Timestamp appears {count} times."
            )
        )

    return issues


def check_missing_values(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
) -> list[dict]:
    """
    Detect missing values by variable and timestamp.
    """
    issues: list[dict] = []

    for column in df.columns:
        missing_mask = df[column].isna()
        missing_timestamps = df.index[missing_mask]

        for timestamp in missing_timestamps:
            issues.append(
                create_issue_record(
                    station_name=station_name,
                    file_name=file_name,
                    issue_type="missing_value",
                    variable=column,
                    timestamp=timestamp,
                    detail=f"Missing value detected in variable '{column}'."
                )
            )

    return issues


def check_temporal_gaps(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    expected_frequency_minutes: int = 10,
) -> list[dict]:
    """
    Detect missing timestamps based on the expected temporal frequency.

    Reports how many records are missing and the missing interval.
    """
    issues: list[dict] = []

    if len(df.index) < 2:
        issues.append(
            create_issue_record(
                station_name=station_name,
                file_name=file_name,
                issue_type="insufficient_temporal_records",
                detail=(
                    f"Only {len(df.index)} timestamp(s) available. "
                    "Temporal continuity cannot be evaluated."
                )
            )
        )
        return issues

    sorted_index = df.index.sort_values()
    expected_delta = pd.Timedelta(minutes=expected_frequency_minutes)

    for previous_timestamp, current_timestamp in zip(sorted_index[:-1], sorted_index[1:]):
        observed_delta = current_timestamp - previous_timestamp

        if observed_delta > expected_delta:
            observed_interval_minutes = int(observed_delta.total_seconds() / 60)
            missing_steps = observed_interval_minutes // expected_frequency_minutes - 1
            missing_start = previous_timestamp + expected_delta
            missing_end = current_timestamp - expected_delta

            if missing_steps == 1:
                detail = f"Missing 1 record at {missing_start}."
            else:
                detail = (
                    f"Missing {missing_steps} records from {missing_start} "
                    f"to {missing_end}."
                )

            issues.append(
                create_issue_record(
                    station_name=station_name,
                    file_name=file_name,
                    issue_type="temporal_gap",
                    timestamp_start=missing_start,
                    timestamp_end=missing_end,
                    missing_count=missing_steps,
                    detail=detail
                )
            )

    return issues

# ============================================================
# Step 2. Expected temporal window logic
# ============================================================

def get_expected_dates(analysis_date: pd.Timestamp) -> tuple[pd.Timestamp, set]:
    """
    Calculate expected temporal coverage from analysis date.

    Expected logic for one processed file:
    - analysis day = D
    - expected start = D-1 at 18:00
    - expected days present = D-1, D, D+1
    """
    analysis_date = pd.to_datetime(analysis_date).normalize()

    expected_start = (analysis_date - pd.Timedelta(days=1)) + pd.Timedelta(hours=18)
    expected_days = {
        (analysis_date - pd.Timedelta(days=1)).date(),
        analysis_date.date(),
        (analysis_date + pd.Timedelta(days=1)).date(),
    }

    return expected_start, expected_days


def check_window_coverage(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    analysis_date: pd.Timestamp,
) -> list[dict]:
    """
    Validate that the dataframe covers the expected temporal window
    derived from the analysis date encoded in the file name.

    Expected logic:
    - expected start: D-1 at 18:00
    - expected days present: D-1, D and D+1
    """
    issues: list[dict] = []

    analysis_date = pd.to_datetime(analysis_date).normalize()
    expected_start, expected_days = get_expected_dates(analysis_date)

    available_days = set(df.index.date)
    missing_days = sorted(expected_days - available_days)

    for missing_day in missing_days:
        issues.append(
            create_issue_record(
                station_name=station_name,
                file_name=file_name,
                issue_type="missing_expected_day",
                detail=f"Expected day {missing_day} is missing from the dataset."
            )
        )

    if df.index.min() > expected_start:
        issues.append(
            create_issue_record(
                station_name=station_name,
                file_name=file_name,
                issue_type="missing_analysis_start",
                timestamp=expected_start,
                detail=(
                    f"Dataset starts at {df.index.min()}, later than expected start "
                    f"{expected_start}."
                )
            )
        )

    next_day = analysis_date + pd.Timedelta(days=1)
    day_after_end = next_day + pd.Timedelta(days=1)

    has_next_day_data = ((df.index >= next_day) & (df.index < day_after_end)).any()

    if not has_next_day_data:
        issues.append(
            create_issue_record(
                station_name=station_name,
                file_name=file_name,
                issue_type="missing_next_day_data",
                detail=f"No records found for day after analysis date {next_day.date()}."
            )
        )

    return issues

# ============================================================
# Step 3. Completeness summary
# ============================================================

def build_completeness_summary(
    station_name: str,
    file_name: str,
    issues: list[dict],
) -> dict:
    """
    Build a compact completeness summary for one station.
    """
    summary = {
        "station_name": station_name,
        "file_name": file_name,
        "missing_value_count": 0,
        "temporal_gap_count": 0,
        "duplicated_timestamp_count": 0,
        "missing_expected_day_count": 0,
        "missing_analysis_start_count": 0,
        "missing_next_day_data_count": 0,
        "total_completeness_issues": len(issues),
        "status": "OK" if len(issues) == 0 else "WARNING",
    }

    for issue in issues:
        issue_type = issue["issue_type"]

        if issue_type == "missing_value":
            summary["missing_value_count"] += 1
        elif issue_type == "temporal_gap":
            summary["temporal_gap_count"] += 1
        elif issue_type == "duplicated_timestamp":
            summary["duplicated_timestamp_count"] += 1
        elif issue_type == "missing_expected_day":
            summary["missing_expected_day_count"] += 1
        elif issue_type == "missing_analysis_start":
            summary["missing_analysis_start_count"] += 1
        elif issue_type == "missing_next_day_data":
            summary["missing_next_day_data_count"] += 1

    return summary

# ============================================================
# Step 4. Completeness check runner
# ============================================================

def run_completeness_checks(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    analysis_date: pd.Timestamp,
    expected_frequency_minutes: int = 10,
) -> tuple[list[dict], dict]:
    """
    Run all completeness checks for a single station dataframe.
    """
    all_issues: list[dict] = []
    # 1. Check duplicated timestamps
    all_issues.extend(check_duplicated_timestamps(df, station_name, file_name))
    
    # 2. Check missing values
    all_issues.extend(check_missing_values(df, station_name, file_name))
    
    # 3. Check temporal gaps
    all_issues.extend(
        check_temporal_gaps(
            df=df,
            station_name=station_name,
            file_name=file_name,
            expected_frequency_minutes=expected_frequency_minutes,
        )
    )
    
    # 4. Check expected window coverage
    all_issues.extend(
        check_window_coverage(
            df=df,
            station_name=station_name,
            file_name=file_name,
            analysis_date=analysis_date,
        )
    )
    
    # 5. Build completeness summary
    summary = build_completeness_summary(
        station_name=station_name,
        file_name=file_name,
        issues=all_issues,
    )

    return all_issues, summary