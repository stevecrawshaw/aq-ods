pacman::p_load(tidyverse, glue, janitor, sf, duckdb, DBI)

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "data/aq.duckdb")

con |> dbListTables()
# icu is for timezones - used to agg by year
dbSendQuery(con, "Install icu;")
dbSendQuery(con, "Load icu;")

aurn_annual_data <- tbl(con, "dim_aurn_tbl") |> 
  filter(site_type == "Urban Traffic") |> 
  inner_join(tbl(con, "fact_aurn_tbl"), by = join_by("code" == "code")) |>
  select(site, code, CAUTH24NM, date, no2, nox, no, o3, pm10, pm2.5) |> 
  group_by(site, code, CAUTH24NM, year = year(date)) |>
  summarise(across(.cols = c(no2, pm10, pm2.5), ~mean(.x, na.rm = TRUE))) |> 
  arrange(CAUTH24NM, site, code, desc(year)) |> 
  collect() |> 
  glimpse()

