pacman::p_load(tidyverse, sf, janitor, glue, readxl)

sgc_path <- "data/SGC 2024 ASR Tables_NO2 Montoring Results.xlsx"

#banes_data <- read_rds("data/banes_air_quality.rds")
#names(banes_data)
sgc_sheets = readxl::excel_sheets(sgc_path)
tube_sheet = keep(sgc_sheets, str_detect(sgc_sheets, "Tube"))

raw_tube_data_tbl <- readxl::read_xlsx(
  sgc_path,
  sheet = tube_sheet,
  skip = 2) |> 
  janitor::clean_names() |> 
  glimpse()


last_row <- which.min(!is.na(raw_tube_data_tbl$site_name)) -1

sgc_aq_data_long_tbl <- raw_tube_data_tbl |> 
  slice(1:last_row) |> 
  mutate(site_id = str_split_i(diffusion_tube_id, ", ", 1) |> 
           str_extract(pattern = "[0-9]+")) |> 
  select(site_id, 
         site_name,
         site_type,
         x_os_grid_ref_easting,
         y_os_grid_ref_northing,
         starts_with("x2")) |>
  pivot_longer(cols = starts_with("x2"),
               names_to = "year",
               values_to = "concentration") |>
  mutate(year = str_remove(year, "x") |> as.integer()) |>
  glimpse()

sgc_aq_data_long_tbl |>
  write_rds("data/sgc_air_quality_long.rds")


# Geometry operations -------------------
