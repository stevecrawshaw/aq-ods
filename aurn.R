pacman::p_load(tidyverse, openair, glue, janitor, arrow)

bristol_aq <- importAURN(site = c("BRS8", "BR11"),
                         year = 2019:2025,
                         pollutant = c("no2", "nox", "no", "o3", "pm10", "pm25"),
                         to_narrow = TRUE)

write_parquet(
  bristol_aq,
  sink = "data/aurn_data.parquet",
  chunk_size = NULL,
  version = "2.4"
)
