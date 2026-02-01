pacman::p_load(tidyverse, openair, janitor, glue, tidyplots)

br11 <- importAURN(site = "BR11", year = 2024)

br11 |>
  calendarPlot(
    year = 2024,
    pollutant = "nox",
    annotate = "ws",
    # cols = "inferno",
    main = "Bristol Temple Way"
  )


br11 |>
  polarPlot(year = 2024, pollutant = "nox", type = "season")

br11 |>
  timeVariation(
    pollutant = "nox",
    type = "season",
    main = "Bristol Temple Way - NOx Time Variation"
  )

install.packages(old.packages())
