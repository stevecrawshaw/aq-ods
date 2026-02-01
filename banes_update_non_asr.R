# Update the BANES data with historical data from outside the ASR
#
pacman::p_load(tidyverse, glue, janitor, readxl, arrow, sf)

# Get source data from ODS
orig_conc_url <- "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/air-quality-measurements/exports/parquet?lang=en"

orig_sites_url = "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/air-quality-monitoring-sites/exports/parquet?lang=en&timezone=Europe%2FLondon"

banes_orig_sites_url = "https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/air-quality-monitoring-sites/exports/parquet?lang=en&refine=la_name%3A%22Bath%20and%20North%20East%20Somerset%22&timezone=Europe%2FLondon"

orig_concs_tbl <- read_parquet(orig_conc_url)
orig_sites_tbl <- read_parquet(orig_sites_url)
banes_orig_sites_tbl <- read_parquet(banes_orig_sites_url)

blob_to_geopoint <- function(blob_col) {
  #' Convert a column of WKB blobs to a vector of geopoint strings
  #' @param blob_col A column of WKB blobs, typically from a `sf` object or geoparquet file
  #' #' @return A character vector of geopoint strings in the format "{lat,long}"  as required for the  ODS rendering of points
  #' @details This function uses `rlang::enquo` to capture the column name and `rlang::eval_tidy` to evaluate it.
  #' The function is designed to be used within a `mutate` call in a `dplyr` pipeline.
  #' to be used within a mutate function call
  bc <- enquo(blob_col)
  # Convert the WKB blobs to simple features geometries
  # The wkb blobs are represented as vectors in R
  geom_list <- map(
    rlang::eval_tidy(bc),
    ~ as.raw(.x) |>
      st_as_sfc(wkb = TRUE)
  )
  # set the length of the vector to the length of the geom_list
  gp_vec <- vector(mode = "character", length = length(geom_list))
  # Iterate over the list to construct the geopoint strings
  gp_vec = map(
    geom_list,
    ~ paste0("{", st_coordinates(.x)[2], ",", st_coordinates(.x)[1], "}")
  )
  # Return the vector of geopoint strings
  unlist(gp_vec)
}

# Get update data from the spreadsheet

banes_update_concs_tbl <- read_xlsx(
  "data/Regional website.xlsx",
  sheet = "Monitoring"
) |>
  glimpse()

banes_update_sites_tbl <- read_xlsx(
  "data/Regional website.xlsx",
  sheet = "Site details"
) |>
  glimpse()

# Identify new sites

new_banes_site_ids <- base::setdiff(
  banes_update_sites_tbl$`Site ID`,
  banes_orig_sites_tbl$site_id
)

# Create update datasets with the right schema
clean_sites_tbl <- orig_sites_tbl |>
  mutate(geo_point_2d = blob_to_geopoint(geo_point_2d), geo_shape = NULL) # recreated by ODS on import

(sites_names <- names(clean_sites_tbl))

new_banes_sites_tbl <- banes_update_sites_tbl |>
  clean_names() |>
  filter(site_id %in% new_banes_site_ids) |>
  rename(
    geo_point_2d = geo_point,
    in_aqma_which_aqm = aqma,
    distance_to_relevant_exposure_m = distance_to_exposure,
    distance_to_kerb_of_nearest_road_m = distance_to_kerb,
    tube_co_located_with_a_continuous_analyser = colocation,
    height_m = height,
    aqma_bool = site_in_aqma,
    aqma = aqma_name,
    la_name = local_authority,
    ladcd = local_authority_code
  ) |>
  select(any_of(sites_names)) |>
  mutate(geo_point_2d = paste0("{", as.character(geo_point_2d), "}")) |>
  glimpse()

year(orig_concs_tbl$year)


clean_concs_tbl <- orig_concs_tbl |>
  mutate(year = year(year), geo_point_2d = NULL)

concs_names = names(clean_concs_tbl)

new_banes_concs_tbl <- banes_update_concs_tbl |>
  clean_names() |>
  mutate(
    year = as.integer(year),
    # ladcd wrong in spreadsheet
    local_authority_code = "E06000022"
  ) |>
  filter(!is.na(no2_annual_mean)) |>
  rename(
    annual_mean_no2 = no2_annual_mean,
    annual_exc_no2 = no2_exceedence,
    daily_exc_pm10 = pm10_exceedence,
    annual_mean_pm10 = pm10_annual_mean,
    annual_mean_pm25 = pm2_5_annual_mean,
    annual_mean_no2_distance_corrected = no2_distance_corrected_annual_mean,
    ladcd = local_authority_code,
    la_name = local_authority_name
  ) |>
  select(any_of(concs_names)) |>
  glimpse()

# Combine the new sites and concentrations with the original data
#
upload_sites_tbl <- bind_rows(
  clean_sites_tbl,
  new_banes_sites_tbl
) |>
  glimpse()

upload_concs_tbl <- bind_rows(
  clean_concs_tbl,
  new_banes_concs_tbl
) |>
  glimpse()

write_csv(upload_sites_tbl, "data/banes_aq_sites_update.csv", na = "")

write_csv(upload_concs_tbl, "data/banes_aq_concs_update.csv", na = "")
