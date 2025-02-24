# base data
# 
asr_la_list <- list(
  "bristol"  = list(
    "la_name" = "Bristol, City of",
    "ladcd" = "E06000023",
    "dt_path" = "data/Bristol DTTool_Entriesv4.0.xlsx",
    "cs_path" = "data/Bristol_City_Council_2024_ASR_Tables.xlsx"
  ),
  "banes" = list(
    "la_name" = "Bath and North East Somerset",
    "ladcd" = "E06000022",
    "dt_path" = "data/Table A-4 2024 ASR.xlsx",
    "cs_path" = "data/Table A-4 2024 ASR.xlsx"
  ),
  "south_glos" = list(
    "la_name" = "South Gloucestershire",
    "ladcd" = "E06000025",
    "dt_path" = "data/SGC 2024 ASR Monitoring Tables.xlsx",
    "cs_path" = "data/SGC 2024 ASR Monitoring Tables.xlsx"
  )
)



# Processing functions ----
# 

get_aqma_bool <- function(col){
  map_lgl(col, ~str_detect(.x, "Yes|yes|y"))
}

get_clean_bracket <- function(col){ 
  
    map_chr(col,
            ~if_else(
              str_detect(.x, "\\("),
              str_extract(.x, "\\(.+\\)") |>
                       str_remove("\\(") |>
                       str_remove("\\)"),
              ""))
      
  }


extract_site_id <- function(tbl, col) {
  # utility function for extracting site IDs from 
  # the range of different SIte ID's in use  UAs
  col_name <- rlang::ensym(col)
  
  tbl  |> 
    dplyr::mutate(!!col_name := stringr::str_extract(
      !!col_name,
      pattern = ".*?\\d+(?=[_a-zA-Z]|$)"
    ))
}

distance_correct <- function(dt_table_dtdes_tbl,
                             year = 2023){
# create the subset of DT data which is distance corrected
# i.e. where the tube position does not reflect exposure
# but concentration is adjusted to represent that exposure
    dt_table_dtdes_tbl |> 
    extract_site_id(site_id) |>
  filter(!is.na(distance_corrected_annual_mean_ug_m3)) |>
  transmute(site_id,
       annual_mean_no2_distance_corrected = distance_corrected_annual_mean_ug_m3,
       year = year)
}

extract_stat_bracket <- function(stat_bracket,
                                 s_or_b = "stat") {
  # separates out the bracketed value from the stat value
  # from ASR tables
  if(!is.na(stat_bracket)){
    if (str_detect(stat_bracket, "\\(|\\)")) {
    out <- str_match_all(
      stat_bracket,
      "\\d+(?:\\.\\d+)?(?=\\s*\\()|(?<=\\()\\d+(?:\\.\\d+)?(?=\\))"
      )

    if (s_or_b == "stat") {
      return(
        as.double(out[[1]][1])
      )
    } else if (s_or_b == "bracket") {
      return(
        as.double(out[[1]][2])
        )
    }
  } else {
    return(stat_bracket |> as.double())
  }
  } else {
    return (NA_real_)
  }
}


process_year_concs <- function(cs_table){
  cs_table |> 
    mutate(
    across(
      starts_with("x2"),
      ~map_dbl(.x,
               ~map_dbl(.x,
                        ~extract_stat_bracket(.x,
                                              "stat")))),
    site_id = as.character(site_id)) |> 
  select(starts_with("x2"), site_id) |> 
  pivot_longer(cols = starts_with("x2"),
               names_to = "year",
               values_to = "concentration") |> 
  mutate(year = str_remove(year, "x") |> as.integer())
}
