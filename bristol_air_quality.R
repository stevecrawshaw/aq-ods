pacman::p_load(tidyverse, sf, janitor, glue, readxl, httpgd)

data_spreadsheet <- "data/2023 BCC AQ Data_Pre ASR Publication.xlsx"

sheets <- readxl::excel_sheets(data_spreadsheet)

tubes_raw <- readxl::read_xlsx(data_spreadsheet, sheet = sheets[[1]]) |>
  janitor::clean_names() |>
  glimpse()

uunn_tube <- tribble(
  ~site_id , ~site_name      , ~x     , ~y     , ~conc_2023 , ~valid ,
       999 , "UUNN_BRIS_009" , 359435 , 173510 , 42.9       , TRUE
) |>
  glimpse()


tubes_cleaner_tbl <- tubes_raw |>
  mutate(
    site_id = if_else(
      str_detect(site_no, "_"),
      str_sub(site_no, 1, 3),
      site_no
    ) |>
      as.integer(),
    valid = !str_detect(x12, ".") | is.na(x12),
    x = as.integer(x),
    y = as.integer(y)
  ) |>
  rename(
    conc_2023 = x2023_annualised_and_bias_adjusted_0_85_not_distance_adjusted
  ) |>
  glimpse()

annual_tube_data_bristol_long_tbl <- tubes_cleaner_tbl |>
  bind_rows(uunn_tube) |>
  filter(valid) |>
  select(-c(site_no, valid_data_capture_2023_percent, x12, valid)) |>
  rename(site_type = site_classification) |>
  pivot_longer(
    cols = matches("x2|conc_"),
    names_to = "year",
    values_to = "concentration"
  ) |>
  mutate(
    year = str_remove(year, "x|conc_") |> as.integer(),
    site_id = as.character(site_id)
  ) |>
  glimpse()

annual_tube_data_bristol_long_tbl |>
  write_rds("data/bristol_no2_annual_long.rds")


tubes_zebra_out_tbl <- tubes_cleaner_tbl |>
  select(site_id, site_name, x, y, conc_2023, valid) |>
  bind_rows(uunn_tube) |>
  glimpse()

tubes_zebra_out_tbl |> tail()

tubes_zebra_out_sf <- tubes_zebra_out_tbl |>
  st_as_sf(coords = c("x", "y"), crs = 27700)

plot(tubes_zebra_out_sf |> select(conc_2023))

st_write(tubes_zebra_out_sf, "data/bristol_no2_dt_2023.shp", append = FALSE)

tubes_zebra_out_sf |>
  filter(valid, conc_2023 > 40) |>
  st_write("data/bristol_no2_dt_2023_gt_40.shp", append = FALSE)
glimpse()
