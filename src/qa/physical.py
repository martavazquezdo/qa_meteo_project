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
) -> dict:
    """
    Build a standardized physical issue record.
    """
    return {
        "station_name": station_name,
        "file_name": file_name,
        "check_dimension": "physical",
        "issue_type": issue_type,
        "variable": variable,
        "timestamp": timestamp,
        "detail": detail,
    }
    
# ============================================================
# Step 1. Basic physical checks
# ============================================================
def check_physical_ranges(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    thresholds: dict[str, dict[str, float | None]],
) -> list[dict]:
    """
    Detect physically impossible or implausible values based on predefined ranges.

    Thresholds must be defined per variable, using:
    {
        "variable_name": {"min": value_or_None, "max": value_or_None}
    }
    """
    issues: list[dict] = []

    if df.empty:
        return issues

    for column, limits in thresholds.items():
        if column not in df.columns:
            continue

        min_value = limits.get("min")
        max_value = limits.get("max")

        series = df[column]

        if min_value is not None:
            low_mask = series < min_value
            for timestamp in series[low_mask].index:
                issues.append(
                    create_issue_record(
                        station_name=station_name,
                        file_name=file_name,
                        issue_type="physical_range_low",
                        variable=column,
                        timestamp=timestamp,
                        detail=(
                            f"Value {series.loc[timestamp]} in '{column}' is below minimum "
                            f"physical threshold {min_value}."
                        )
                    )
                )

        if max_value is not None:
            high_mask = series > max_value
            for timestamp in series[high_mask].index:
                issues.append(
                    create_issue_record(
                        station_name=station_name,
                        file_name=file_name,
                        issue_type="physical_range_high",
                        variable=column,
                        timestamp=timestamp,
                        detail=(
                            f"Value {series.loc[timestamp]} in '{column}' is above maximum "
                            f"physical threshold {max_value}."
                        )
                    )
                )

    return issues


def check_rain_low_humidity(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    rain_column: str,
    rh_column: str,
    rain_min: float,
    rh_min: float,
) -> list[dict]:
    """
    Detect potential physical inconsistencies where precipitation is recorded
    together with unusually low relative humidity.

    This does not necessarily indicate an error, but a situation that may require review.
    """
    issues: list[dict] = []

    if rain_column not in df.columns or rh_column not in df.columns:
        return issues

    rain_series = df[rain_column]
    rh_series = df[rh_column]

    # Condition: rain present but humidity too low
    mask = (rain_series > rain_min) & (rh_series < rh_min)

    flagged_timestamps = df.index[mask]

    for timestamp in flagged_timestamps:
        issues.append(
            create_issue_record(
                station_name=station_name,
                file_name=file_name,
                issue_type="rain_low_humidity_inconsistency",
                variable=f"{rain_column}-{rh_column}",
                timestamp=timestamp,
                detail=(
                    f"Precipitation ({rain_series.loc[timestamp]}) detected with low relative humidity "
                    f"({rh_series.loc[timestamp]}%). "
                    f"Thresholds: rain > {rain_min}, RH < {rh_min}."
                )
            )
        )

    return issues

# ============================================================
# Step 2. Physical summary
# ============================================================
def build_physical_summary(
    station_name: str,
    file_name: str,
    issues: list[dict],
) -> dict:
    """
    Build a compact physical summary for one station.
    """
    summary = {
        "station_name": station_name,
        "file_name": file_name,
        "range_low_count": 0,
        "range_high_count": 0,
        "rain_low_humidity_count": 0,
        "total_physical_issues": len(issues),
        "status": "OK" if len(issues) == 0 else "WARNING",
    }

    for issue in issues:
        issue_type = issue["issue_type"]

        if issue_type == "physical_range_low":
            summary["range_low_count"] += 1
        elif issue_type == "physical_range_high":
            summary["range_high_count"] += 1
        elif issue_type == "rain_low_humidity_inconsistency":
            summary["rain_low_humidity_count"] += 1

    return summary
# ============================================================
# Step 3. Physical check runner
# ============================================================

def run_physical_checks(
    df: pd.DataFrame,
    station_name: str,
    file_name: str,
    physical_range_thresholds: dict[str, dict[str, float | None]],
    rain_low_humidity_thresholds: dict[str, float],
) -> tuple[list[dict], dict]:
    """
    Run all physical consistency checks for a single station dataframe.

    Pipeline steps:
    1. Check physical ranges
    2. Check rain with low humidity
    3. Build compact physical summary
    """
    all_issues: list[dict] = []


    # 1. Check physical ranges
    all_issues.extend(
        check_physical_ranges(
            df=df,
            station_name=station_name,
            file_name=file_name,
            thresholds=physical_range_thresholds,
        )
    )

    # 2. Check rain with low humidity

    all_issues.extend(
        check_rain_low_humidity(
            df=df,
            station_name=station_name,
            file_name=file_name,
            rain_column="rain_l_m2",
            rh_column="rh_pct",
            rain_min=rain_low_humidity_thresholds["rain_min"],
            rh_min=rain_low_humidity_thresholds["rh_min"],
        )
    )


    # 3. Build compact physical summary
    summary = build_physical_summary(
        station_name=station_name,
        file_name=file_name,
        issues=all_issues,
    )

    return all_issues, summary
