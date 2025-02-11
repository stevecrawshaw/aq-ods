pacman::p_load(tidyverse, sf, janitor, glue)

raw_dt_lst <- read_rds("data/bristol_raw_dt_lst.rds")
raw_cs_lst <- read_rds("data/bristol_raw_cs_lst.rds")

list2env(raw_dt_lst, envir = .GlobalEnv)
list2env(raw_cs_lst, envir = .GlobalEnv)

# Processing functions ----
# 

extract_site_id <- function(tbl, col) {
  # utility function for extracting site IDs from 
  # the range of different SIte ID's in use  UAs
  col_name <- rlang::ensym(col)
  
  tbl  |> 
    dplyr::mutate(!!col_name := stringr::str_extract(
      !!col_name,
      pattern = ".*?\\d+(?=[_a-zA-Z]|$)"
    ))
}

distance_correct <- function(dt_table_dtdes_tbl,
                             year = 2023){
# create the subset of DT data which is distance corrected
# i.e. where the tube position does not reflect exposure
# but concentration is adjusted to represent that exposure
    dt_table_dtdes_tbl |> 
    extract_site_id(site_id) |>
  filter(!is.na(distance_corrected_annual_mean_ug_m3)) |>
  transmute(site_id,
       annual_mean_no2_distance_corrected = distance_corrected_annual_mean_ug_m3,
       year = year)
  
}

extract_stat_bracket <- function(stat_bracket,
                                 s_or_b = "stat") {
  # separates out the bracketed value from the stat value
  # from ASR tables
  if(!is.na(stat_bracket)){
    if (str_detect(stat_bracket, "\\(|\\)")) {
    out <- str_match_all(
      stat_bracket,
      "\\d+(?:\\.\\d+)?(?=\\s*\\()|(?<=\\()\\d+(?:\\.\\d+)?(?=\\))"
      )

    if (s_or_b == "stat") {
      return(
        as.double(out[[1]][1])
      )
    } else if (s_or_b == "bracket") {
      return(
        as.double(out[[1]][2])
        )
    }
  } else {
    return(stat_bracket |> as.double())
  }
  } else {
    return (NA_real_)
  }
}


process_year_concs <- function(cs_table){
  cs_table |> 
    mutate(
    across(
      starts_with("x2"),
      ~map_dbl(.x,
               ~map_dbl(.x,
                        ~extract_stat_bracket(.x,
                                              "stat")))),
    site_id = as.character(site_id)) |> 
  select(starts_with("x2"), site_id) |> 
  pivot_longer(cols = starts_with("x2"),
               names_to = "year",
               values_to = "concentration") |> 
  mutate(year = str_remove(year, "x") |> as.integer())
}

# Extract Monitoring Site (DIM) Data -------------------

# Get the DT sites ----
# 
n_tubes_tbl <- dt_table_dtdes_tbl |> 
  extract_site_id(site_id) |> 
  select(site_id, single_duplicate_triplicate)

dt_sites_tbl <- dt_table_a2_tbl |> 
  mutate(
    site_id = diffusion_tube_id,
  across(.cols = c(x_os_grid_ref_easting,
                          y_os_grid_ref_northing,
                          distance_to_relevant_exposure_m,
                          distance_to_kerb_of_nearest_road_m,
                          height_m),
                .fns = as.integer),
  monitoring_technique = "Diffusion tube") |> 
  extract_site_id(site_id) |>
  left_join(n_tubes_tbl, by = "site_id") |>
  glimpse()

# Get the Continuous Site (DIM) Data ----
  
cs_sites_tbl <- cs_table_a1 |> 
  rename_with(.fn = ~str_remove(.x, "_[0-9]$"), .cols = starts_with("distance"))|>
  rename("height_m" = "inlet_height_m") |> 
  mutate(across(.cols = c(x_os_grid_ref_easting,
                          y_os_grid_ref_northing,
                          distance_to_relevant_exposure_m,
                          distance_to_kerb_of_nearest_road_m,
                          height_m),
                .fns = as.integer)) |>
  glimpse()

# Join the continuous and diffusion tube data ----

all_sites_tbl <- dt_sites_tbl |> 
  bind_rows(cs_sites_tbl) |> 
  mutate(
    # catch instances where nothing entered in colocation column
    tube_co_located_with_a_continuous_analyser =
      if_else(is.na(tube_co_located_with_a_continuous_analyser) & monitoring_technique == "Diffusion tube",
              "No",
              tube_co_located_with_a_continuous_analyser),
    # convert to boolean
    tube_co_located_with_a_continuous_analyser =
      str_detect(tube_co_located_with_a_continuous_analyser,
                         "Yes|yes|y"),
    la_name = la_name,
    ladcd = ladcd) |> 
  # geometry for export as lat long
  # geojson import creates the geoshape directly
  st_as_sf(coords = c("x_os_grid_ref_easting",
                      "y_os_grid_ref_northing"),
           crs = 27700) |>
  st_transform(4326) |>
  glimpse()

# Write the consolidated data for MONITORING SITES ----

all_sites_tbl |>
  st_write("data/bristol_aq_sites.geojson",
           driver = "GeoJSON", delete_dsn = TRUE)


# Extract Monitoring Data (FACT) Data ------------
# 
# 

dt_concs_am <- dt_table_a4_tbl |> 
  extract_site_id(diffusion_tube_id) |>
  rename(site_id = diffusion_tube_id) |>
  select(site_id, starts_with("x2")) |>
  pivot_longer(cols = starts_with("x2"),
                names_to = "year",
                values_to = "annual_mean_no2") |>
  mutate(year = str_remove(year, "x") |> as.integer()) |>
  glimpse()


dt_concs_am_distance_corrected <- distance_correct(
  # only the most recent year is distance corrected
  dt_table_dtdes_tbl,
  year = 2023) |> 
  glimpse()

dt_list <- list(
  dt_concs_am,
  dt_concs_am_distance_corrected
)

dt_tbl <- dt_list |> 
  reduce(left_join, by = c("site_id", "year")) |> 
  # remove rows where there is no data for that site and year
  filter(!is.na(annual_mean_no2) |
         !is.na(annual_mean_no2_distance_corrected)) |>
  glimpse()

# Continuous sites ----

cs_concs_no2_am <- cs_table_a3 |> 
  process_year_concs() |> 
  rename("annual_mean_no2" = "concentration") |>
  glimpse()
  
cs_concs_no2_exc <- cs_table_a5 |> 
  process_year_concs() |> 
  rename("annual_exc_no2" = "concentration") |>
  glimpse()

cs_concs_pm10_am <- cs_table_a6 |> 
  process_year_concs() |> 
  rename("annual_mean_pm10" = "concentration") |>
  glimpse()

cs_concs_pm10_exc <- cs_table_a7 |> 
  process_year_concs() |> 
  rename("daily_exc_pm10" = "concentration") |>
  glimpse()

cs_concs_pm25_am <- cs_table_a8 |> 
  process_year_concs() |> 
  rename("annual_mean_pm25" = "concentration") |>
  glimpse()

cs_list <- list(
  cs_concs_no2_am,
  cs_concs_no2_exc,
  cs_concs_pm10_am,
  cs_concs_pm10_exc,
  cs_concs_pm25_am
)

cs_tbl <- cs_list |> 
  reduce(left_join, by = c("site_id", "year"))

bind_rows(cs_tbl, dt_tbl) |>
  mutate(ladcd = ladcd) |> 
  write_csv("data/bristol_aq_concs.csv", na = "")
