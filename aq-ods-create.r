pacman::p_load(tidyverse, sf, janitor, glue)

raw_dt_lst <- read_rds("data/bristol_raw_dt_lst.rds")
raw_cs_lst <- read_rds("data/bristol_raw_cs_lst.rds")

list2env(raw_dt_lst, envir = .GlobalEnv)
list2env(raw_cs_lst, envir = .GlobalEnv)

# Extract Monitoring Site (DIM) Data -------------------

# Get the DT sites ----

dt_sites_tbl <- dt_table_a2_tbl |> 
  mutate(site_id = str_extract(diffusion_tube_id,
                               pattern = "^[0-9]+"),
  across(.cols = c(x_os_grid_ref_easting,
                          y_os_grid_ref_northing,
                          distance_to_relevant_exposure_m,
                          distance_to_kerb_of_nearest_road_m,
                          height_m),
                .fns = as.integer),
  monitoring_technique = "Diffusion tube") |> 
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
    site_id = as.integer(site_id),
    # catch instances where nothing enetered in colocation column
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

# Write the consolidated data ----

all_sites_tbl |>
  st_write("data/bristol_aq_sites.geojson",
           driver = "GeoJSON")
