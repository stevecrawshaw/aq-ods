-- Create a duckdb database based on the data from Bristol datasette instance (frozen at early 2023)
-- Includes background grid data from background grid mapping gov data
-- And some recent AURN data from UK-_air
-- This data is used to inform presentation for air quality in WECA

duckdb

-- INSTALL SPATIAL;
LOAD SPATIAL;
LOAD HTTPFS;

-- Connect to MCA's Postgres database VPN ON!!!!!
-- credentials are stored in a secret manager
-- We retrieve the background grids and transform to enable joining and mapping

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

ATTACH 'data/aq.duckdb' AS aq;
USE aq;

SHOW TABLES;

SELECT table_name FROM information_schema.columns
GROUP BY table_name, table_catalog, table_schema
HAVING table_catalog = 'weca_postgres' AND table_schema = 'bcc';

-- Boundaries of CAZ and AQMAs from various sources into the aq database

CREATE OR REPLACE TABLE aq.bcc_caz AS
SELECT * EXCLUDE(shape),
     ST_GeomFromWKB(weca_postgres.bcc.caz.shape).ST_Transform('EPSG:27700', 'EPSG:4326').ST_FlipCoordinates() AS geom
 FROM weca_postgres.bcc.caz;

CREATE OR REPLACE TABLE aq.banes_caz AS
SELECT 
     ST_GeomFromWKB(weca_postgres.banes.caz.shape).ST_Transform('EPSG:27700', 'EPSG:4326').ST_FlipCoordinates() AS geom
FROM weca_postgres.banes.caz;

-- make a single file containing both CAZ boundaries
COPY(
SELECT *, 'Bath and North East Somerset' ladnm, 'E06000022' ladcd, 'C' caz_class FROM aq.banes_caz
UNION BY NAME
SELECT geom, 'Bristol, City of' ladnm, 'E06000023' ladcd, 'D' caz_class FROM aq.bcc_caz)
TO 'data/weca_caz.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');

-- create a single table for all the AQMAs in the region
CREATE OR REPLACE TABLE aq.weca_aqmas AS
SELECT * REPLACE(ST_Transform(geom, 'EPSG:27700', 'EPSG:4326').ST_FlipCoordinates().ST_MakeValid() AS geom)
     FROM ST_read("data/AQMA_ShapeFile/All_pollutants.shp")
     WHERE la_ons_nam IN  ('South Gloucestershire', 'Bath and North East Somerset', 'Bristol, City of');

-- Export to GeoJSON for AQMA
COPY (SELECT * FROM aq.banes_caz) TO 'data/banes_caz.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY (SELECT * FROM aq.bcc_caz) TO 'data/bcc_caz.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
COPY (SELECT * FROM aq.weca_aqmas) TO 'data/weca_aqmas.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');


-- background grids for the whole of southern england
CREATE OR REPLACE TABLE aq.background_grids_base_tbl AS
SELECT weca_postgres.os.grid_1km.plan_no,
ST_GeomFromWKB(weca_postgres.os.grid_1km.shape) AS geom,
ST_GeomFromWKB(weca_postgres.os.grid_1km.shape).ST_Centroid().ST_X().round(0)::INTEGER::VARCHAR || '_' ||
ST_GeomFromWKB(weca_postgres.os.grid_1km.shape).ST_Centroid().ST_Y().round(0)::INTEGER::VARCHAR id  
FROM weca_postgres.os.grid_1km
WHERE plan_no LIKE 'SS%'
     OR plan_no LIKE 'ST%'
     OR plan_no LIKE 'SX%'
     OR plan_no LIKE  'SY%';

-- background concentrations. read in all csv files and union them
-- group by to get a shorter table
CREATE OR REPLACE TABLE aq.background_concs_tbl AS
(SELECT Local_Auth_Code, id: x || '_' || y, SUM(COLUMNS('^Tot'))
FROM read_csv('data/from_datasette/background/*.csv',
union_by_name = TRUE)
GROUP BY Local_Auth_Code, id);

DETACH weca_postgres;

-- join the background grids and concentrations in a view
CREATE OR REPLACE VIEW background_grids_concs_vw AS
(SELECT *
FROM background_grids_base_tbl g
INNER JOIN background_concs_tbl c
ON g.id = c.id);

-- export for mapping
COPY background_grids_concs_vw TO
'data/background_grids_condc.fgb'
WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');

-- Data below are from the datasette air quality data for Bristol

CREATE OR REPLACE TABLE no2_tube_concs_raw_tbl AS
SELECT * FROM read_csv('data/from_datasette/_no2_tubes_raw__202502281345.csv');

CREATE OR REPLACE TABLE continuous_tbl AS
SELECT site_id, date_time, nox, no, no2, pm10, pm25, o3, temp, press, rh
FROM read_csv('data/from_datasette/_air_quality_data_continuous__202502281343.csv');

CREATE OR REPLACE TABLE monitoring_sites_tbl AS
SELECT * EXCLUDE(point_geom), ST_Point(longitude, latitude) geom FROM 
read_csv('data/from_datasette/_air_quality_monitoring_sites__202502281344.csv');

FROM monitoring_sites_tbl WHERE location ILIKE '%aurn%' LIMIT 2;

CREATE OR REPLACE TABLE no2_annual_tbl AS
SELECT * FROM read_csv('data/from_datasette/_no2_diffusion_tube_data__202502281344.csv');

-- AURN data for all combined authorities
-- separate aurn table to include weather data
-- sourced from aurn.R - as openair needed to source data
CREATE OR REPLACE TABLE dim_aurn_tbl AS
SELECT * EXCLUDE(FID, source),
ST_Point(LONG, LAT) as geom,
CASE
 WHEN code = 'BR11' THEN 500
 WHEN code = 'BRS8' THEN 452
 ELSE NULL END site_id,
FROM read_parquet('data/aurn_sites_in_ca.parquet');

ALTER TABLE dim_aurn_tbl
ADD PRIMARY KEY (code);

-- CREATE OR REPLACE TABLE fact_aurn_tbl AS
-- SELECT * FROM read_parquet('data/aurn_data_ca.parquet');

DROP TABLE IF EXISTS fact_aurn_tbl;

CREATE OR REPLACE TABLE fact_aurn_tbl(code VARCHAR,
                         date TIMESTAMP WITH TIME ZONE,
                         nox DOUBLE,
                         no2 DOUBLE,
                         "no" DOUBLE,
                         o3 DOUBLE,
                         so2 DOUBLE,
                         ws DOUBLE,
                         wd DOUBLE,
                         air_temp DOUBLE,
                         pm10 DOUBLE,
                         "pm2.5" DOUBLE,
                         v10 DOUBLE,
                         "v2.5" DOUBLE,
                         nv10 DOUBLE,
                         "nv2.5" DOUBLE,
                         co DOUBLE);

INSERT INTO fact_aurn_tbl
SELECT * FROM read_parquet('data/aurn_data_ca.parquet');

-- Data from Bristol's "open data portal"
-- These are continuous data from Bristol's sites from 1993 to 2023
.quit
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

CREATE OR REPLACE TABLE dim_aq_contin_sites_bristol_tbl AS
SELECT SITE_ID site_id,
       "LOCATION" "location",
       EASTING easting,
       NORTHING northing,
       ST_Point(easting, northing).ST_Transform('EPSG:27700', 'EPSG:4326') as geom,
       '{' || geom.ST_X()::VARCHAR || ', ' || geom.ST_Y()::VARCHAR || '}' AS geo_point_2d
FROM  aq
GROUP BY ALL;

ALTER TABLE dim_aq_contin_sites_bristol_tbl
ADD PRIMARY KEY (site_id);

CREATE OR REPLACE TABLE fact_aq_contin_concs_bristol_tbl(site_id BIGINT,
                                             datetime TIMESTAMP,
                                             "no" FLOAT,
                                             nox FLOAT,
                                             no2 FLOAT,
                                             "pm2.5" FLOAT,
                                             pm10 FLOAT);

--CREATE OR REPLACE TABLE fact_aq_contin_concs_bristol_tbl AS
INSERT INTO fact_aq_contin_concs_bristol_tbl
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


-- SELECT * 
-- FROM fact_aq_contin_concs_bristol_tbl f
-- INNER JOIN dim_aq_contin_sites_bristol_tbl d
-- USING (site_id);
-- WHERE f.site_id = 188 AND f.datetime BETWEEN date'1996-01-01' AND date'1997-01-01';


DROP TABLE aq;

COPY fact_aq_contin_concs_bristol_tbl TO 'data/fact_aq_contin_concs_bristol_tbl.parquet' (FORMAT PARQUET);
COPY dim_aq_contin_sites_bristol_tbl TO 'data/dim_aq_contin_sites_bristol_tbl.parquet' (FORMAT PARQUET);

FROM aq.dim_aq_contin_sites_bristol_tbl;
SELECT DISTINCT site_id FROM aq.fact_aq_contin_concs_bristol_tbl;


ATTACH 'md:';
-- md authentication is in user environment variables

SHOW DATABASES;

CREATE OR REPLACE DATABASE air_quality FROM aq;

SHOW ALL TABLES;

USE air_quality;
.mode duckbox


CREATE OR REPLACE MACRO glimpse(table_name) AS TABLE
       WITH schema_tbl AS
       (SELECT name,
       unnest(column_names) column_name,
       unnest(column_types) "type"
       FROM (SHOW ALL TABLES) )
       SELECT * EXCLUDE(name) FROM schema_tbl
       INNER JOIN 
       (UNPIVOT
        (SELECT list(COLUMNS(*)::VARCHAR) 
            FROM query_table(table_name) LIMIT 5)
        ON COLUMNS(*)
        INTO NAME column_name
        VALUE sample_data) as sample_tbl
        USING (column_name)
        WHERE schema_tbl.name = table_name;

FROM glimpse('weca_aqmas');

ATTACH 'md:macros';

SHOW TABLES;

FROM macros.glimpse('weca_aqmas');


.shell git add . && git commit -m 'macro'
.shell git push origin main

.tables
.quit