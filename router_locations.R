# join router locations to sites for RMS router map

library(tidyverse)
library(tidylog)

routers_tbl <- read_delim(
  "data/2022-02-07 20_15_02_devices_export.csv",
  delim = ",",
  col_types = cols_only(serial = col_integer(), name = col_character())
)

sites_tbl <- read_delim(
  "https://opendata.bristol.gov.uk/explore/dataset/air-quality-monitoring-sites/download/?format=csv&disjunctive.pollutants=true&refine.current=True&refine.pollutants=NOX&refine.pollutants=PM2.5&refine.pollutants=PM10&timezone=Europe/London&lang=en&use_labels_for_header=false&csv_separator=%3B",
  delim = ";",
  col_types = cols_only(siteid = col_integer(), geo_point_2d = col_character())
)
router_loc_tbl <-
  routers_tbl |>
  filter(name != "RUT950_Spare") |>
  transmute(
    siteid = str_sub(name, start = -3L, end = -1L) |>
      as.integer(),
    serial
  ) |>
  inner_join(sites_tbl, by = "siteid") |>
  separate(
    geo_point_2d,
    into = c("latitude", "longitude"),
    sep = ",",
    remove = TRUE
  ) |>
  select(-siteid)

router_loc_tbl |>
  write_csv(file = "data/router_loc.csv")
