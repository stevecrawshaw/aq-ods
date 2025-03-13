duckdb

ATTACH 'data/bristol_aq.duckdb' AS aq;
USE aq;

INSTALL SPATIAL;
LOAD SPATIAL;

SHOW TABLES;

CREATE OR REPLACE TABLE continuous_tbl AS
SELECT * FROM read_csv_auto('data/from_datasette/_air_quality_data_continuous__202502281343.csv');

CREATE OR REPLACE TABLE no2_dt_raw_tbl AS
SELECT * FROM read_csv_auto('data/from_datasette/_no2_tubes_raw__202502281345.csv');


CREATE OR REPLACE TABLE no2_annual_tbl AS
SELECT * FROM read_csv_auto('data/from_datasette/_no2_diffusion_tube_data__202502281344.csv');

DROP TABLE background_grids_tbl;

CREATE OR REPLACE TABLE background_grids_tbl AS
SELECT * FROM st_read('data/from_datasette/Background_grids_pollutant_mapping.geojson');



SHOW TABLES;

DESCRIBE background_grids_tbl;


.quit