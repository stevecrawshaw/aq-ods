-- Extract all the postcodes in each of the CAZs in the West of England Combined Authority (WECA) area
-- and save them in a format that can be used in GIS applications
-- Customer - CMA for CAZ evaluation on local economy

duckdb

LOAD SPATIAL;

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);
SHOW TABLES;

SELECT table_name FROM information_schema.columns
GROUP BY table_name, table_catalog, table_schema
HAVING table_catalog = 'weca_postgres' AND table_schema = 'os';

CREATE OR REPLACE TABLE bcc_caz AS
SELECT *, shape.ST_GeomFromWKB().ST_Transform('EPSG:27700', 'EPSG:4326') Geometry FROM weca_postgres.bcc.caz;

CREATE OR REPLACE TABLE banes_caz AS
SELECT *, shape.ST_GeomFromWKB().ST_Transform('EPSG:27700', 'EPSG:4326') Geometry FROM weca_postgres.banes.caz;

CREATE OR REPLACE TABLE postcodes AS
(SELECT postcode, lat, lon, shape.ST_GeomFromWKB().ST_Transform('EPSG:27700', 'EPSG:4326') Geometry FROM weca_postgres.os.codepoint_open);

SHOW TABLES;

CREATE OR REPLACE TABLE bcc_caz_postcodes AS
SELECT postcode, lat, lon, postcodes.Geometry FROM postcodes 
INNER JOIN bcc_caz
ON ST_Within(postcodes.Geometry, bcc_caz.Geometry);

CREATE OR REPLACE TABLE banes_caz_postcodes AS
SELECT postcode, lat, lon, postcodes.Geometry FROM postcodes 
INNER JOIN banes_caz
ON ST_Within(postcodes.Geometry, banes_caz.Geometry);

FROM banes_caz_postcodes LIMIT 10;

COPY (FROM bcc_caz_postcodes) TO 'data/bcc_caz_postcodes.csv' WITH (FORMAT CSV, HEADER TRUE);
COPY (FROM banes_caz_postcodes) TO 'data/banes_caz_postcodes.csv' WITH (FORMAT CSV, HEADER TRUE);

SELECT * FROM ST_Drivers() WHERE can_create = true AND short_name IN ('FlatGeobuf', 'GeoJSON');

COPY (SELECT postcode, Geometry.ST_Transform('EPSG:4326', 'EPSG:27700') FROM bcc_caz_postcodes) TO 'data/bcc_caz_postcodes.fgb' WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');
COPY (SELECT postcode, Geometry.ST_Transform('EPSG:4326', 'EPSG:27700') FROM banes_caz_postcodes) TO 'data/banes_caz_postcodes.fgb' WITH (FORMAT GDAL, DRIVER 'FlatGeobuf');
