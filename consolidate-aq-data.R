pacman::p_load(tidyverse, sf, glue, janitor)

# read in the bristol data
b <- read_rds("data/bristol_aq_data.rds")
# read in the South Glos data
s <- read_rds("data/s_glos_aq_data.rds")

ba <- read_rds("data/banes_aq_data.rds")

list(b, s, ba) |> walk(~ list2env(.x, envir = .GlobalEnv))

all_sites_tbl <- bind_rows(
  bristol_all_sites_tbl,
  s_glos_all_sites_tbl,
  banes_all_sites_tbl
) |>
  glimpse()

all_concs_tbl <- bind_rows(
  bristol_aq_concs_tbl,
  s_glos_aq_concs_tbl,
  banes_aq_concs_tbl
) |>
  glimpse()

all_sites_tbl |>
  st_write("data/all_sites_tbl.geojson", driver = "GeoJSON", delete_dsn = TRUE)

all_concs_tbl |>
  write_csv("data/all_concs_tbl.csv", na = "")
