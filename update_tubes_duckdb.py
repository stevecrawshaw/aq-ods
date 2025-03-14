# %%
"""
Update the historic no2 annual mean data in the bristol air quality database
data/aq.duckdb with the latest data provided by the UA's published on the
open data portal. The data is retrieved using the HTTPFS extension and the
loaded into a temporary table. The data is pre - filtered by the API
to just bristol. Then in this script the last year in the historic data is retrieved
and used as filtering criterion to only insert the new data. The new data is then
inserted into the historic table.
"""

# %%
import duckdb
import yaml

# %%

con = duckdb.connect(database="data/aq.duckdb", read_only=False)
con.sql("INSTALL HTTPFS;")
con.sql("LOAD HTTPFS;")
con.sql("SHOW TABLES;")


# %%
# retrieve the credentials from the config file
# as the air quality data is private for now.


def load_config(config_path: str) -> dict:
    """
    Function to retrieve credentials from a config file.
    """
    try:
        with open(config_path) as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        print("Config file not found.")
        raise


# %%
apikey = load_config("../config.yml")["ods"]["apikey"]
print(apikey)
# %%
# construct the url to the data
tube_url = f"https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/bristol_aq_concs/exports/csv?select=site_id%2Cyear%2Cannual_mean_no2&limit=-1&refine=la_name%3A%27Bristol%2C%20City%20of%27&timezone=UTC&use_labels=false&epsg=4326&apikey={apikey}"

# %%
max_year = con.query("SELECT MAX(year) FROM no2_annual_tbl;").fetchone()[0]
print("The latest year in the no2_annual_tbl is:", max_year)
# %%
qry = f"""
        SELECT site_id, year, annual_mean_no2 no2
        FROM read_csv('{tube_url}') WHERE year > {max_year};
        """
update_tbl = con.query(qry)
update_tbl
# %%
con.query("SELECT COUNT(*) FROM update_tbl;")
# %%
con.query("SELECT COUNT(*) FROM no2_annual_tbl;")
# %%
# insert the new data into the annual mean table
con.query("INSERT INTO no2_annual_tbl SELECT * FROM update_tbl;")
# %%
con.query("SELECT COUNT(*) FROM no2_annual_tbl;")
# %%
con.close()
# %%
