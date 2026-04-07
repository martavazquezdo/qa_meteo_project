import pandas as pd
from src.validation.completeness import check_temporal_gaps
from src.validation.physical import check_rain_low_humidity


def test_temporal_gap():
    df = pd.DataFrame({
        "temp": [10, 11],
    }, index=pd.to_datetime([
        "2026-01-08 00:00",
        "2026-01-08 00:30",  # gap de 20 min (esperado 10)
    ]))

    issues = check_temporal_gaps(
        df=df,
        station_name="StationA",
        file_name="StationA_08_01_2026.csv",
        expected_frequency_minutes=10,
    )

    assert len(issues) == 1
    assert issues[0]["issue_type"] == "temporal_gap"
    assert issues[0]["missing_count"] == 2
    

def test_rain_low_humidity():
    df = pd.DataFrame({
        "rain_l_m2": [0.0, 5.0],
        "rh_pct": [80, 20],
    }, index=pd.to_datetime([
        "2026-01-08 00:00",
        "2026-01-08 00:10",
    ]))

    issues = check_rain_low_humidity(
        df=df,
        station_name="StationA",
        file_name="StationA_08_01_2026.csv",
        rain_column="rain_l_m2",
        rh_column="rh_pct",
        rain_min=1.0,
        rh_min=30.0,
    )

    assert len(issues) == 1
    assert issues[0]["issue_type"] == "rain_low_humidity_inconsistency"