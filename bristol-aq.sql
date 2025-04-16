nu
http get 'https://maps.bristol.gov.uk/opendata/ContinuousAirQuality2018-2023.zip' | save data/ContinuousAirQuality.zip
unzip data/ContinuousAirQuality.zip -d data/


duckdb

.mode duckbox
CREATE OR REPLACE TABLE aq AS
-- SELECT column_name, column_type from (DESCRIBE
SELECT *,
substring(DATE_TIME, 12, 2).regexp_matches('24') AS wrong,
split_part(DATE_TIME, ' ', 1) AS date_str,
split_part(DATE_TIME, ' ', 2).substring(1, 5) AS time_str
FROM read_csv('data/ContinuousAirQuality.csv',
columns = {
    "SITE_ID": "BIGINT",
    "LOCATION": "VARCHAR",
    "EASTING": "BIGINT",
    "NORTHING": "BIGINT",
    "DATE_TIME": "VARCHAR",
    "NO": "FLOAT",
    "NOX": "FLOAT",
    "NO2": "FLOAT",
    "PM2_5": "FLOAT",
    "PM10": "FLOAT"
},
timestampformat = "%d/%m/%Y %H:%M:%S",
ignore_errors = true); 

CREATE OR REPLACE TABLE fact_aq_concs_tbl AS
WITH d AS
(SELECT *, CASE
    WHEN wrong THEN CAST((strptime(date_str, '%d/%m/%Y') + INTERVAL 1 DAY) AS DATE)
    ELSE CAST(strptime(date_str, '%d/%m/%Y') AS DATE)
END AS "date",
CASE
    WHEN wrong THEN CAST('00:00' AS TIME)
    ELSE CAST(strptime(time_str, '%H:%M') AS TIME)
END AS "time",
"date"::DATE + "time"::time AS "datetime"
 FROM aq)
 SELECT SITE_ID site_id,
        datetime,
        NO "no",
        NOX nox,
        NO2 no2,
        PM2_5 "pm2.5",
        PM10 pm10
FROM d;

CREATE OR REPLACE TABLE dim_aq_sites_tbl AS
SELECT SITE_ID,
       "LOCATION",
       EASTING,
       NORTHING
FROM  aq
GROUP BY ALL;


COPY fact_aq_concs_tbl TO 'data/fact_aq_concs_tbl.parquet' (FORMAT PARQUET);
COPY dim_aq_sites_tbl TO 'data/dim_aq_sites_tbl.parquet' (FORMAT PARQUET);

.quit