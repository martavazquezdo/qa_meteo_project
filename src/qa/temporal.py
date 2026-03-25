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
    event_count: int | None = None,
) -> dict:
    """
    Build a standardized temporal issue record.
    """
    return {
        "station_name": station_name,
        "file_name": file_name,
        "check_dimension": "temporal",
        "issue_type": issue_type,
        "variable": variable,
        "timestamp": timestamp,
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
        "event_count": event_count,
        "detail": detail,
    }
    

# ============================================================
# Step 1. Basic temporal checks
# ============================================================

def check_abrupt_changes(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    thresholds: dict[str, float],
) -> list[dict]:
    """
    Detect abrupt changes between consecutive timestamps for selected variables.

    Thresholds are expressed in absolute units per timestep.
    """
    issues: list[dict] = []

    if df.empty:
        return issues

    df = df.sort_index()

    for column, threshold in thresholds.items():
        if column not in df.columns:
            continue

        series = df[column]
        diffs = series.diff().abs()
        flagged = diffs > threshold

        for timestamp in diffs[flagged].index:
            issues.append(
                create_issue_record(
                    station_name=station_name,
                    file_name=file_name,
                    issue_type="abrupt_change",
                    variable=column,
                    timestamp=timestamp,
                    detail=(
                        f"Abrupt change detected in '{column}'. "
                        f"Observed absolute change: {diffs.loc[timestamp]:.2f}. "
                        f"Threshold: {threshold:.2f}."
                    )
                )
            )

    return issues

def check_constant_value_persistence(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    thresholds: dict[str, int],
    exclude_zero_columns: list[str] | None = None,
) -> list[dict]:
    """
    Detect suspicious constant values maintained over too many consecutive steps.

    Thresholds are defined per variable because persistence sensitivity
    depends on sensor resolution and expected physical variability.

    For selected variables, zero-value runs can be excluded.
    """
    issues: list[dict] = []

    if df.empty:
        return issues

    if exclude_zero_columns is None:
        exclude_zero_columns = []

    for column, min_consecutive_steps in thresholds.items():
        if column not in df.columns:
            continue

        series = df[column].sort_index()

        if series.empty:
            continue

        # Identify groups of consecutive identical values
        group_id = (series != series.shift()).cumsum()

        for _, group in series.groupby(group_id):
            # Ignore groups containing missing values
            if group.isna().any():
                continue

            constant_value = group.iloc[0]

            # Ignore zero-runs for selected variables (e.g. rainfall)
            if column in exclude_zero_columns and constant_value == 0:
                continue

            if len(group) >= min_consecutive_steps:
                issues.append(
                    create_issue_record(
                        station_name=station_name,
                        file_name=file_name,
                        issue_type="constant_value_persistence",
                        variable=column,
                        timestamp_start=group.index.min(),
                        timestamp_end=group.index.max(),
                        event_count=len(group),
                        detail=(
                            f"Constant value persistence detected in '{column}'. "
                            f"Value {constant_value} repeated for {len(group)} consecutive records. "
                            f"Threshold: {min_consecutive_steps}."
                        )
                    )
                )

    return issues

def check_isolated_spikes(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    thresholds: dict[str, float],
) -> list[dict]:
    """
    Detect isolated spikes where the central value differs strongly from both neighbors,
    while neighbors remain relatively coherent.

    Thresholds are defined per variable and expressed in absolute units.
    """
    issues: list[dict] = []

    if df.empty:
        return issues

    for column, threshold in thresholds.items():
        if column not in df.columns:
            continue

        series = df[column].sort_index()

        if len(series) < 3:
            continue

        prev_diff = (series - series.shift(1)).abs()
        next_diff = (series - series.shift(-1)).abs()
        neighbor_diff = (series.shift(1) - series.shift(-1)).abs()

        flagged = (
            (prev_diff > threshold) &
            (next_diff > threshold) &
            (neighbor_diff <= threshold)
        )

        for timestamp in series[flagged].index:
            issues.append(
                create_issue_record(
                    station_name=station_name,
                    file_name=file_name,
                    issue_type="isolated_spike",
                    variable=column,
                    timestamp=timestamp,
                    detail=(
                        f"Isolated spike detected in '{column}'. "
                        f"Value differs strongly from both neighboring timestamps. "
                        f"Threshold: {threshold}."
                    )
                )
            )

    return issues

# ============================================================
# Step 2. Temporal summary
# ============================================================

def build_temporal_summary(
    station_name: str,
    file_name: str,
    issues: list[dict],
) -> dict:
    """
    Build a compact temporal summary for one station.
    """
    summary = {
        "station_name": station_name,
        "file_name": file_name,
        "abrupt_change_count": 0,
        "constant_value_persistence_count": 0,
        "isolated_spike_count": 0,
        "total_temporal_issues": len(issues),
        "status": "OK" if len(issues) == 0 else "WARNING",
    }

    for issue in issues:
        issue_type = issue["issue_type"]

        if issue_type == "abrupt_change":
            summary["abrupt_change_count"] += 1
        elif issue_type == "constant_value_persistence":
            summary["constant_value_persistence_count"] += 1
        elif issue_type == "isolated_spike":
            summary["isolated_spike_count"] += 1

    return summary

# ============================================================
# Step 3. Temporal check runner
# ============================================================
def run_temporal_checks(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    abrupt_thresholds: dict[str, float],
    spike_thresholds: dict[str, float],
    persistence_thresholds: dict[str, int],
    exclude_zero_columns: list[str] | None = None,
) -> tuple[list[dict], dict]:
    """
    Run all temporal consistency checks for a single station dataframe.

    Pipeline steps:
    1. Check abrupt changes
    2. Check constant value persistence
    3. Check isolated spikes
    4. Build compact temporal summary
    """
    all_issues: list[dict] = []

    if exclude_zero_columns is None:
        exclude_zero_columns = []

    # 1. Check abrupt changes
    all_issues.extend(
        check_abrupt_changes(
            df=df,
            station_name=station_name,
            file_name=file_name,
            thresholds=abrupt_thresholds,
        )
    )

    # 2. Check constant value persistence
    all_issues.extend(
        check_constant_value_persistence(
            df=df,
            station_name=station_name,
            file_name=file_name,
            thresholds=persistence_thresholds,
            exclude_zero_columns=exclude_zero_columns,
        )
    )

    # 3. Check isolated spikes
    all_issues.extend(
        check_isolated_spikes(
            df=df,
            station_name=station_name,
            file_name=file_name,
            thresholds=spike_thresholds,
        )
    )

    # 4. Build temporal summary
    summary = build_temporal_summary(
        station_name=station_name,
        file_name=file_name,
        issues=all_issues,
    )

    return all_issues, summary
