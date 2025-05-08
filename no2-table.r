pacman::p_load(tidyverse, gt, gtExtras, glue, janitor, openair, DBI, duckdb)
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "data/aq.duckdb")

dbListTables(con)

tbl(con, "no2_annual_tbl") |> 
  glimpse()
dbDisconnect(con)
