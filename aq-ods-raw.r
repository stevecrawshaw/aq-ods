# First the xlsb files supplied by the UA's as Defra binary Excel
# files (xlsb) are converted to xlsx files using the python script
# aq-ods-xlsb.py. The xlsx files are then read into R using this script.
# 
pacman::p_load(tidyverse, janitor, glue, readxl, jsonlite)

source("aq-processing-functions.r")
# testing
# dt_path <- asr_la_list |> 
#   pluck("bristol") |> 
#   pluck("dt_path")


# Diffusion Tube Tables -----

# given a list of LA data, extract the raw data from the spreadsheets and store the df's in a list along with the metadata
# Split out by diffusion tubes and continuous sites
# 
get_raw_dt_la_lst <- function(la_list) {
  # function to retrieve the diffusion tube raw data from the DT spreadsheet
  la_name <- pluck(la_list, "la_name")
  dt_path <- pluck(la_list, "dt_path")
  ladcd <- pluck(la_list, "ladcd")

  dt_step_2_inputs_tbl <- 
  read_xlsx(dt_path,
            sheet = "STEP 2 - Diffusion Tube Inputs",
            skip = 11) |> 
  clean_names(replace = c("\u00b5" = "u")) |>
  select(-matches("x\\d")) |>
  select(diffusion_tube_id:requires_annualisation) |>
  filter(!is.na(diffusion_tube_id)) 

dt_table_a2_tbl <- 
  read_xlsx(dt_path,
            sheet = "Table A.2",
            skip = 8) |> 
  clean_names(replace = c("\u00b5" = "u")) |>
  select(-matches("x\\d")) |>
  filter(!is.na(diffusion_tube_id)) 


dt_a4_names_parameters <- read_xlsx(dt_path,
                                 sheet = "Table A.4",
                                 range = "C9:H9") |> 
  clean_names(replace = c("\u00b5" = "u")) |>
  names()

dt_a4_names_years <- read_xlsx(dt_path,
                            sheet = "Table A.4",
                            range = "I10:M10") |> 
  clean_names(replace = c("\u00b5" = "u")) |>
  names()

(dt_a4_names <- c(dt_a4_names_parameters, dt_a4_names_years))

dt_table_a4_tbl <- 
  read_xlsx(dt_path,
            sheet = "Table A.4",
            skip = 10) |> 
  select(1:11) |> 
  set_names(dt_a4_names) |>
  filter(!is.na(diffusion_tube_id)) 

dt_table_dtdes_tbl <- read_xlsx(dt_path,
                      sheet = "DTDES Inputs",
                      skip = 1) |> 
  clean_names(replace = c("\u00b5" = "u")) |>
  select(-matches("x\\d")) |>
  filter(!is.na(unique_id))


out_list <- list(
  "dt_step_2_inputs_tbl" = dt_step_2_inputs_tbl,
  "dt_table_a2_tbl" = dt_table_a2_tbl,
  "dt_table_a4_tbl" = dt_table_a4_tbl,
  "dt_table_dtdes_tbl" = dt_table_dtdes_tbl,
  "la_name" = la_name,
  "ladcd" = ladcd
)

return(out_list)
  
}

banes_raw_dt_lst <- pluck(asr_la_list, "banes") |> 
  get_raw_dt_la_lst()

# test the function
bristol_raw_dt_lst <- pluck(asr_la_list, "bristol") |> 
  get_raw_dt_la_lst()


# Continuous Site Tables ------
# site metadata

get_raw_cs_la_lst <- function(la_list){
  
  la_name <- pluck(la_list, "la_name")
  cs_path <- pluck(la_list, "cs_path")
  ladcd <- pluck(la_list, "ladcd")


cs_table_a1 <- read_xlsx(cs_path,
                         sheet = "Table A.1",
                         skip = 4) |> 
  clean_names(replace = c("\u00b5" = "u"))

# annual mean concentrations NO2
cs_table_a3 <- read_xlsx(cs_path,
                         sheet = "Table A.3",
                         skip = 4) |>
  clean_names(replace = c("\u00b5" = "u")) 

# 1 hour means NO2

cs_table_a5 <- read_xlsx(cs_path,
                         sheet = "Table A.5",
                         skip = 4) |>
  clean_names(replace = c("\u00b5" = "u")) |> 
  glimpse()
  
# PM10 annual mean concentrations
# 

cs_table_a6 <- read_xlsx(cs_path,
                         sheet = "Table A.6",
                         skip = 4) |>
  clean_names(replace = c("\u00b5" = "u")) |> 
  glimpse()

# PM10 24 hour means
# 

cs_table_a7 <- read_xlsx(cs_path,
                         sheet = "Table A.7",
                         skip = 4) |>
  clean_names(replace = c("\u00b5" = "u")) |> 
  glimpse()

# PM2.5 annual mean concentrations
# 

cs_table_a8 <- read_xlsx(cs_path,
                         sheet = "Table A.8",
                         skip = 4) |>
  clean_names(replace = c("\u00b5" = "u")) |> 
  glimpse()

out_list <- list(
  "cs_table_a1" = cs_table_a1,
  "cs_table_a3" = cs_table_a3,
  "cs_table_a5" = cs_table_a5,
  "cs_table_a6" = cs_table_a6,
  "cs_table_a7" = cs_table_a7,
  "cs_table_a8" = cs_table_a8,
  "la_name" = la_name,
  "ladcd" = ladcd
)
  
return(out_list)  
}

bristol_raw_cs_lst <- pluck(asr_la_list, "bristol") |> 
  get_raw_cs_la_lst()

banes_raw_cs_lst <- pluck(asr_la_list, "banes") |>
  get_raw_cs_la_lst()


bristol_raw_cs_lst |> jsonlite::write_json("data/bristol_raw_cs_lst.json", simplifyDataFrame = FALSE)

write_rds(bristol_raw_dt_lst, "data/bristol_raw_dt_lst.rds")
write_rds(bristol_raw_cs_lst, "data/bristol_raw_cs_lst.rds")

write_rds(banes_raw_dt_lst, "data/banes_raw_dt_lst.rds")
write_rds(banes_raw_cs_lst, "data/banes_raw_cs_lst.rds")

  