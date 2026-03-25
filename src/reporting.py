from __future__ import annotations

from pathlib import Path
import pandas as pd


# ============================================================
# Step 0. Issue enrichment utilities
# ============================================================
def enrich_issues_with_reporting_fields(
    issues_df: pd.DataFrame,
    rules: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """
    Add severity and recommended_action columns to issues dataframe
    based on issue_type rules.
    """
    if issues_df.empty:
        issues_df = issues_df.copy()
        issues_df["severity"] = pd.Series(dtype="object")
        issues_df["recommended_action"] = pd.Series(dtype="object")
        return issues_df

    enriched_df = issues_df.copy()

    enriched_df["severity"] = enriched_df["issue_type"].map(
        lambda x: rules.get(x, {}).get("severity", "UNKNOWN")
    )
    enriched_df["recommended_action"] = enriched_df["issue_type"].map(
        lambda x: rules.get(x, {}).get("recommended_action", "review_required")
    )

    return enriched_df

# ============================================================
# Step 1. Execution-level reporting
# ============================================================

def build_execution_summary(
    issues_df: pd.DataFrame,
    completeness_summary_df: pd.DataFrame,
    temporal_summary_df: pd.DataFrame,
    physical_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a one-row execution summary dataframe.
    """
    stations_processed = max(
        len(completeness_summary_df),
        len(temporal_summary_df),
        len(physical_summary_df),
    )

    if issues_df.empty:
        summary = {
            "stations_processed": stations_processed,
            "total_issues": 0,
            "high_severity_count": 0,
            "medium_severity_count": 0,
            "low_severity_count": 0,
            "status": "OK",
        }
        return pd.DataFrame([summary])

    high_count = (issues_df["severity"] == "HIGH").sum()
    medium_count = (issues_df["severity"] == "MEDIUM").sum()
    low_count = (issues_df["severity"] == "LOW").sum()

    if high_count > 0:
        status = "CRITICAL"
    elif medium_count > 0 or low_count > 0:
        status = "WARNING"
    else:
        status = "OK"

    summary = {
        "stations_processed": stations_processed,
        "total_issues": len(issues_df),
        "high_severity_count": int(high_count),
        "medium_severity_count": int(medium_count),
        "low_severity_count": int(low_count),
        "status": status,
    }

    return pd.DataFrame([summary])

# ============================================================
# Step 2. Station-level reporting
# ============================================================
def build_station_overview(issues_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build station-level reporting overview based on enriched issues dataframe.
    """
    if issues_df.empty:
        return pd.DataFrame(
            columns=[
                "station_name",
                "total_issues",
                "high_severity_count",
                "medium_severity_count",
                "low_severity_count",
                "status",
            ]
        )

    grouped = issues_df.groupby("station_name")

    rows = []
    for station_name, group in grouped:
        high_count = (group["severity"] == "HIGH").sum()
        medium_count = (group["severity"] == "MEDIUM").sum()
        low_count = (group["severity"] == "LOW").sum()

        if high_count > 0:
            status = "CRITICAL"
        elif medium_count > 0 or low_count > 0:
            status = "WARNING"
        else:
            status = "OK"

        rows.append(
            {
                "station_name": station_name,
                "total_issues": len(group),
                "high_severity_count": int(high_count),
                "medium_severity_count": int(medium_count),
                "low_severity_count": int(low_count),
                "status": status,
            }
        )

    return pd.DataFrame(rows).sort_values(
        by=["high_severity_count", "medium_severity_count", "low_severity_count"],
        ascending=False,
    )

# ============================================================
# Step 3. Human-readable report generation
# ============================================================

def generate_execution_report_text(
    execution_id: str,
    reference_date: pd.Timestamp,
    execution_summary_df: pd.DataFrame,
    station_overview_df: pd.DataFrame,
) -> str:
    """
    Generate a simple human-readable execution report.
    """
    summary = execution_summary_df.iloc[0]

    lines = []
    lines.append("QA EXECUTION REPORT")
    lines.append(f"Execution ID: {execution_id}")
    lines.append(f"Reference date: {pd.to_datetime(reference_date).date()}")
    lines.append("")

    lines.append("GLOBAL SUMMARY")
    lines.append(f"- Stations processed: {summary['stations_processed']}")
    lines.append(f"- Total issues: {summary['total_issues']}")
    lines.append(f"- High severity: {summary['high_severity_count']}")
    lines.append(f"- Medium severity: {summary['medium_severity_count']}")
    lines.append(f"- Low severity: {summary['low_severity_count']}")
    lines.append(f"- Overall status: {summary['status']}")
    lines.append("")

    lines.append("STATION OVERVIEW")
    if station_overview_df.empty:
        lines.append("- No issues detected.")
    else:
        for _, row in station_overview_df.iterrows():
            lines.append(
                f"- {row['station_name']}: status={row['status']}, "
                f"total={row['total_issues']}, "
                f"HIGH={row['high_severity_count']}, "
                f"MEDIUM={row['medium_severity_count']}, "
                f"LOW={row['low_severity_count']}"
            )

    return "\n".join(lines)

# ============================================================
# Step 4. Report persistence
# ============================================================
def save_text_report(report_text: str, output_path: Path) -> None:
    """
    Save plain text execution report.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding="utf-8")