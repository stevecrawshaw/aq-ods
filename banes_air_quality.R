pacman::p_load(tidyverse, sf, janitor, glue, readxl)

banes_path <- "data/Table A-4 2024 ASR.xlsx"

banes_sheets = readxl::excel_sheets(banes_path)

raw_aq_data_tbl <- readxl::read_xlsx(banes_path, sheet = banes_sheets) |> 
  janitor::clean_names() |> 
  glimpse()

(first_row <- raw_aq_data_tbl[1,] |> 
  as.character() |> 
  na_if("NA"))

raw_names <- names(raw_aq_data_tbl)

valid_raw_names <- raw_names |> 
  map_chr(~if_else(str_detect(.x, "^no|^x[0-9]"), NA, .x))

new_names <- coalesce(first_row, raw_names) |> 
  map_chr(~if_else(str_detect(.x, "^[0-9]"), str_c("_", .x), .x)) 

aq_data_long_tbl <- raw_aq_data_tbl |> 
  slice(-1) |> 
  set_names(new_names) |>
  mutate(across(starts_with("_"), ~str_remove(.,"-") |>
                  as.double())) |>
 pivot_longer(cols = starts_with("_"), names_to = "year", values_to = "concentration") |>
  mutate(year = str_remove(year, "_") |> as.integer()) |>
 glimpse()


banes_aq_data_long_clean_tbl <- aq_data_long_tbl |> 
  mutate(site_id = if_else(str_detect(diffusion_tube_id,
                                      ", "),
                                      str_sub(diffusion_tube_id, 1, 5),
         diffusion_tube_id)) |> 
  relocate(site_id, .after = diffusion_tube_id) |>
  select(-c(starts_with("valid"), diffusion_tube_id)) |> 
  glimpse()

banes_aq_data_long_clean_tbl |>
  write_rds("data/banes_air_quality_long.rds")


banes_aq_data_sf <- banes_aq_data_long_clean_tbl |> 
  pivot_wider(id_cols = c(starts_with("site"), ends_with("ing")),
              names_from = "year", 
              values_from = "concentration") |> 
  st_as_sf(coords = c("x_os_grid_ref_easting",
                      "y_os_grid_ref_northing"),
           crs = 27700) 

banes_aq_data_sf |> 
  write_rds("data/banes_air_quality.rds")

banes_aq_data_sf |> 
  st_write("data/banes_air_quality.shp")


