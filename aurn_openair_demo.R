pacman::p_load(tidyverse, openair, janitor, glue)

# Get the sites in Birmingham

birmingham_sites <- importMeta(source = "aurn", year = 2024) |> 
  filter(grepl("Birmingham", site)) |> 
  select(code, site, site_type, latitude, longitude) |> 
    glimpse()

# get the codes for these sites as a vector

brum_codes <- pull(birmingham_sites, code)


# Now download the data from these sites for 2024

brum_aurn_tbl <- importAURN(site = brum_codes, year = 2024)

brum_aurn_tbl |> head()

brum_aurn_tbl |> 
  glimpse()


# Show a summaryplot of NOx from the sites

summaryPlot(brum_aurn_tbl,
            pollutant = "nox", 
            year = 2024, 
            ylab = "NOx (ug/m3)",
            main = "Birmingham AURN Sites",
            xlab = "Date"
)

# Now a calendar plot for one site

brum_aurn_tbl |> 
  filter(code == "BIRR") |>
  calendarPlot(year = 2024, pollutant = "nox", 
               annotate = "ws",
               # cols = "inferno",
               main = "Birmingham AURN Sites")


# Lets look at a time series for the last 10 years for one site
# 

br_10_years <- importAURN(site = "BIRR", year = 2014:2024) |> 
  glimpse()

# find first date
min(br_10_years$date)

smoothTrend(br_10_years,
            pollutant = "no2",
            year = 2016:2024,
            ylab = "NO2 (ug/m3)",
            ref.y = list(h = 40, lty = 5),
            main = "Birmingham A4540 Roadside",
            xlab = "Date")

# Look at UK-AIR

# look at open data portal




