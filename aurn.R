pacman::p_load(tidyverse, openair, janitor, glue, sf, nanoparquet, sfarrow)

ca_boundaries <- st_read(
  "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Combined_Authorities_May_2023_Boundaries_EN_BGC/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
)
# get aurn sites running in 2024
aurns <- importMeta(source = "aurn", year = 2024) |>
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326)

aurn_sites_in_ca_tbl <- st_intersection(aurns, ca_boundaries)

aurn_codes_in_ca <- aurn_sites_in_ca_tbl |>
  select(code) |>
  st_drop_geometry() |>
  pull()

aurn_ca_tbl <- importAURN(
  site = aurn_codes_in_ca,
  verbose = TRUE,
  year = 2019:2024
)

aurn_ca_tbl |>
  group_by(code, year = year(date)) |>
  summarise(no2 = mean(no2, na.rm = TRUE)) |>
  glimpse()

aurn_ca_tbl |>
  select(-c(source, site)) |>
  write_parquet("data/aurn_data_ca.parquet")

# geoparquet with sf arrow doesn't have version so duckdb import fails
aurn_sites_in_ca_tbl |>
  select(-c(Shape__Length, Shape__Area, GlobalID)) |>
  st_drop_geometry() |>
  write_parquet("data/aurn_sites_in_ca.parquet")
