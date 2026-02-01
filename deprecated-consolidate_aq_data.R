pacman::p_load(tidyverse, sf)

bristol_aq_tbl <- read_rds("data/bristol_no2_annual_long.rds") |>
  mutate(local_authority = "Bristol, City of") |>
  rename(x_os_grid_ref_easting = x, y_os_grid_ref_northing = y) |>
  glimpse()

banes_aq_tbl <- read_rds("data/banes_air_quality_long.rds") |>
  mutate(local_authority = "Bath and North East Somerset")

sgc_aq_tbl <- read_rds("data/sgc_air_quality_long.rds") |>
  mutate(local_authority = "South Gloucestershire")

consolidated_no2_dt_aq_weca_tbl <- bind_rows(
  bristol_aq_tbl,
  banes_aq_tbl,
  sgc_aq_tbl
) |>
  mutate(
    local_authority = fct_relevel(
      local_authority,
      "Bristol, City of",
      "Bath and North East Somerset",
      "South Gloucestershire"
    )
  ) |>
  filter(!is.na(concentration)) |>
  st_as_sf(
    coords = c("x_os_grid_ref_easting", "y_os_grid_ref_northing"),
    crs = 27700
  ) |>
  st_transform(4326) |>
  glimpse()

consolidated_no2_dt_aq_weca_tbl |>
  st_write(
    "data/consolidated_no2_dt_aq_weca.geojson",
    driver = "GeoJSON",
    delete_dsn = TRUE
  )
