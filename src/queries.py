import duckdb
from contextlib import contextmanager
from pathlib import Path

@contextmanager
def db():
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()

FILE = Path(__file__).parent.parent / "data" / "processed" / "flights.parquet"

def _dynamic_where(filters):
    return f"WHERE {' AND '.join(filters)}" if filters else ""

def delay_trend(filters=None):
    """Chart 1: monthly delay rate time tread, all carriers combined"""
    return f"""
    SELECT
        MAKE_DATE(Year, Month, 1) AS month,
        ROUND(SUM(CASE WHEN DepDelay > 15 OR ArrDelay > 15 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS delay_rate
    FROM '{FILE}'
    {_dynamic_where(filters)}
    GROUP BY Year, Month
    ORDER BY month
    """

def delay_by_carrier(filters=None):
    """Chart 2: delay rate by carrier, ordered worst to best"""
    return f"""
    SELECT
        Reporting_Airline,
        COUNT(*) AS total_flights,
        ROUND(SUM(CASE WHEN DepDelay > 15 OR ArrDelay > 15 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS delay_rate
    FROM '{FILE}'
    {_dynamic_where(filters)}
    GROUP BY Reporting_Airline
    ORDER BY delay_rate DESC
    """

def delay_causes(filters=None):
    """Chart 3: delay cause collapsed into 4 intuitive buckets"""
    return f"""
    SELECT
        SUM(CarrierDelay + LateAircraftDelay) AS airline_issues,
        SUM(WeatherDelay)                     AS weather,
        SUM(NASDelay)                         AS air_traffic,
        SUM(SecurityDelay)                    AS security_other
    FROM '{FILE}'
    {_dynamic_where(filters)}
    """

def delay_by_carrier_and_cause(filters=None):
    """Chart 4: carrier delay rate by cause proportional to tot_delay min"""
    return f"""
    WITH base AS (
        SELECT
            Reporting_Airline,
            ROUND(SUM(CASE WHEN DepDelay > 15 OR ArrDelay > 15 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS delay_rate,
            SUM(CarrierDelay + LateAircraftDelay) AS airline_issues,
            SUM(WeatherDelay)                     AS weather,
            SUM(NASDelay)                         AS air_traffic,
            SUM(SecurityDelay)                    AS security_other
        FROM '{FILE}'
        {_dynamic_where(filters)}
        GROUP BY Reporting_Airline
    ),
    totals AS (
        SELECT *,
            NULLIF(airline_issues + weather + air_traffic + security_other, 0) AS total_cause_minutes
        FROM base
    )
    SELECT
        Reporting_Airline,
        delay_rate * airline_issues / total_cause_minutes AS airline_issues_rate,
        delay_rate * weather        / total_cause_minutes AS weather_rate,
        delay_rate * air_traffic    / total_cause_minutes AS air_traffic_rate,
        delay_rate * security_other / total_cause_minutes AS security_rate
    FROM totals
    ORDER BY delay_rate DESC
    """

def delay_by_dimension(dimension='Origin Airport', filters=None, top_n=20, ascending=False):
    """Chart 5 & 60: top N/20 airports OR routes by delay rate"""
    if dimension == 'Route':
        dim_col = "CONCAT(Origin, ' -> ', Dest)"
        dim_alias = "route"
        group_by = "Origin, Dest"
    elif dimension == 'Origin Airport':
        dim_col = "CONCAT(Origin, ' (', ANY_VALUE(OriginCityName), ')')"
        dim_alias = "airport"
        group_by = "Origin"
    else:  # Destination Airport
        dim_col = "CONCAT(Dest, ' (', ANY_VALUE(DestCityName), ')')"
        dim_alias = "airport"
        group_by = "Dest"
    return f"""
    SELECT
        {dim_col} AS {dim_alias},
        COUNT(*) AS total_flights,
        ROUND(SUM(CASE WHEN DepDelay > 15 OR ArrDelay > 15 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS delay_rate
    FROM '{FILE}'
    {_dynamic_where(filters)}
    GROUP BY {group_by}
    HAVING COUNT(*) >= 100
    ORDER BY delay_rate {'ASC' if ascending else 'DESC'}
    LIMIT {top_n}
    """
