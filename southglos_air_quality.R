pacman::p_load(tidyverse, sf, janitor, glue, readxl)

source("aq-processing-functions.r")

southglos_path <- asr_la_list |> 
  pluck("south_glos") |> 
  pluck("dt_path")

ladcd = asr_la_list |> 
  pluck("south_glos") |> 
  pluck("ladcd")

la_name = asr_la_list |> 
  pluck("south_glos") |> 
  pluck("la_name")

(sheets <- readxl::excel_sheets(southglos_path))

# Diffusion tubes SITES ----------

dt_sites_raw <- readxl::read_xlsx(southglos_path,
                                  sheet = "A.2 Diff Tube Site Details ",
                                  range = "A3:K102") |> 
  janitor::clean_names() |> 
  glimpse()


dt_sites_tbl <- dt_sites_raw |>
    mutate(
    site_id = diffusion_tube_id,
  across(.cols = c(x_os_grid_ref_easting,
                          y_os_grid_ref_northing,
                          distance_to_relevant_exposure_m_1,
                          tube_height_m),
                .fns = as.numeric),
  distance_to_kerb_of_nearest_road_m = map_dbl(
    distance_to_kerb_of_nearest_road_m_2,
    ~extract_stat_bracket(.x, "stat") |> as.numeric()),
  distance_to_kerb_of_nearest_road_m_2 = NULL,
  monitoring_technique = "Diffusion tube",
  aqma = get_clean_bracket(in_aqma_which_aqma),
  aqma_bool = get_aqma_bool(in_aqma_which_aqma),
  tube_co_located_with_a_continuous_analyser  = str_detect(tube_co_located_with_a_continuous_analyser, "Yes|yes|y"),
  tube_count = map_int(diffusion_tube_id,
                          ~str_split(.x, ", ") |>
                     pluck(1) |> 
                     length())) |> 
  rename(
    distance_to_relevant_exposure_m = "distance_to_relevant_exposure_m_1",
    height_m = tube_height_m
    ) |>
  extract_site_id(site_id) |>
  glimpse()

# Continuous analysers SITES ------

cs_sites_raw <- readxl::read_xlsx(southglos_path,
                                  sheet = "A.1 Automatic Site Details",
                                  range = "A3:K6") |> 
  janitor::clean_names() |> 
  glimpse()


cs_sites_tbl <- cs_sites_raw |> 
  mutate(
  across(.cols = c(x_os_grid_ref_easting,
                   y_os_grid_ref_northing),
                .fns = as.numeric),
  aqma = get_clean_bracket(in_aqma_which_aqma),
  aqma_bool = get_aqma_bool(in_aqma_which_aqma),
  height_m = str_extract_all(inlet_height_m, "\\d\\.\\d") |> 
           map(as.double) |> map(mean) |> unlist(),
  distance_to_relevant_exposure_m = na_if(distance_to_relevant_exposure_m_1, "N/A") |> as.double(),
  pollutants_monitored = str_replace_all(pollutants_monitored, "\r\n", " "),
  monitoring_technique = str_replace_all(
    monitoring_technique,
    c("\r\n" = " ",
      "Chemiluminescent" = "Chemiluminescent (NOx)")
    )) |>
  rename(distance_to_kerb_of_nearest_road_m = distance_to_kerb_of_nearest_road_m_2) |>
  select(-c(inlet_height_m, in_aqma_which_aqma,
            distance_to_relevant_exposure_m_1)) |> 
  glimpse()

#TODO  - sort out in aqma types
s_glos_all_sites_tbl <- dt_sites_tbl |>
  bind_rows(cs_sites_tbl) |>
  mutate(la_name = la_name,
         ladcd = ladcd) |>
  st_as_sf(coords = c("x_os_grid_ref_easting",
                      "y_os_grid_ref_northing"),
           crs = 27700) |>
  st_transform(4326) |>
  glimpse()

s_glos_all_sites_tbl |>
  st_write("data/s_glos_aq_sites.geojson",
           driver = "GeoJSON", delete_dsn = TRUE)

# Diffusion Tubes concentration data ----
# 
dt_data_raw <- readxl::read_xlsx(southglos_path,
                                sheet = "A.4 Diff Tube NO2 Annual Means",
                                range = "A3:L102") |> 
  janitor::clean_names() |> 
  glimpse()


dt_concs_no2_am <- dt_data_raw |>
  extract_site_id(diffusion_tube_id) |>
  rename(site_id = diffusion_tube_id) |>
  select(site_id, starts_with("x2")) |>
  pivot_longer(cols = starts_with("x2"),
                names_to = "year",
                values_to = "annual_mean_no2") |>
  mutate(year = str_remove(year, "x") |> as.integer()) |>
  glimpse()

# Continuous sites concentrations ----

cs_concs_no2_am <- readxl::read_xlsx(southglos_path,
                                    sheet = "A.3 Automatic NO2 Annual Means",
                                    range = "A3:L6") |> 
  janitor::clean_names() |> 
  process_year_concs() |> 
  rename("annual_mean_no2" = "concentration") |>
  glimpse()


cs_concs_pm10_am <- readxl::read_xlsx(southglos_path,
                                    sheet = "A.6 PM10 Annual Means",
                                    range = "A3:L5") |> 
  janitor::clean_names() |> 
  process_year_concs() |> 
  rename("annual_mean_pm10" = "concentration") |>
  glimpse()

cs_concs_pm25_am <- readxl::read_xlsx(southglos_path,
                                    sheet = "A.8 PM2.5 Annual Means",
                                    range = "A3:L4") |> 
  janitor::clean_names() |> 
  process_year_concs() |> 
  rename("annual_mean_pm25" = "concentration") |>
  glimpse()

# Write the consolidated concentrations data ----
# 

s_glos_aq_concs_tbl <- bind_rows(
  dt_concs_no2_am,
  cs_concs_no2_am,
  cs_concs_pm10_am,
  cs_concs_pm25_am
) |> 
  filter(!is.na(annual_mean_no2) |
         !is.na(annual_mean_pm10) |
         !is.na(annual_mean_pm25)) |>
  mutate(ladcd = ladcd) 



write_rds(list("s_glos_aq_concs_tbl" = s_glos_aq_concs_tbl, "s_glos_all_sites_tbl" = s_glos_all_sites_tbl),
          "data/s_glos_aq_data.rds")
