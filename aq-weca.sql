-- Create a duckdb database based on the data from Bristol datasette instance (frozen at early 2023)
-- Includes background grid data from background grid mapping gov data
-- And some recent AURN data from UK-_air
-- This data is used to inform presentation for air quality in WECA

duckdb


INSTALL SPATIAL;
LOAD SPATIAL;

-- Connect to MCA's Postgres database VPN ON!!!!!
-- credentials are stored in a secret manager
-- We retrieve the background grids and transform to enable joining and mapping

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

ATTACH 'data/aq.duckdb' AS aq;
USE aq;

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

CREATE OR REPLACE TABLE no2_tube_concs_raw_tbl AS
SELECT * FROM read_csv('data/from_datasette/_no2_tubes_raw__202502281345.csv');

CREATE OR REPLACE TABLE continuous_tbl AS
SELECT date_time, nox, no, no2, pm10, pm25, o3, temp, press, rh
FROM read_csv('data/from_datasette/_air_quality_data_continuous__202502281343.csv');

CREATE OR REPLACE TABLE monitoring_sites_tbl AS
SELECT * EXCLUDE(point_geom), ST_Point(longitude, latitude) geom FROM 
read_csv('data/from_datasette/_air_quality_monitoring_sites__202502281344.csv');

FROM monitoring_sites_tbl WHERE location ILIKE '%aurn%' LIMIT 2;

CREATE OR REPLACE TABLE no2_annual_tbl AS
SELECT * FROM read_csv('data/from_datasette/_no2_diffusion_tube_data__202502281344.csv');

-- separate aurn table to include wind speed and direction 
CREATE OR REPLACE TABLE aurn_tbl AS
SELECT * EXCLUDE(source), CASE
 WHEN code = 'BR11' THEN 500
 WHEN code = 'BRS8' THEN 452
 ELSE 0 END site_id
FROM read_parquet('data/aurn_data.parquet');

.tables
.quit