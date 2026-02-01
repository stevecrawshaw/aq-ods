pacman::p_load(tidyverse, gt, gtExtras, glue, janitor, openair, DBI, duckdb)
# connect to duckdb
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "data/aq.duckdb")

dbListTables(con)
# get historic data for bristol
bris_no2_early_tbl <- tbl(con, "no2_annual_tbl") |>
  collect()
# filter on just high no2 sites in 2010
table_site_id_vec <- bris_no2_early_tbl |>
  filter(year == 2010, no2 >= 60) |>
  collect() |>
  pull(site_id)

bris_no2_early_tbl <- bris_no2_early_tbl |>
  filter(site_id %in% table_site_id_vec, between(year, 2010, 2018))

# get the later data for bristol only from ASR via ODS and duckdb
bris_no2_late_tbl <- tbl(con, "air_quality_measurements_ods_tbl") |>
  filter(la_name == "Bristol, City of") |>
  transmute(
    year = year(year),
    site_id = as.integer(site_id),
    no2 = annual_mean_no2
  ) |>
  collect()

bris_all_long_tbl <-
  bind_rows(bris_no2_early_tbl, bris_no2_late_tbl) |>
  filter(!is.na(no2)) |>
  arrange(site_id, year) |>
  glimpse()

bris_all_wide_tbl <- bris_all_long_tbl |>
  pivot_wider(
    names_from = year,
    values_from = no2
  ) |>
  na.omit() |>
  glimpse()

sparklines_tbl <- bris_all_long_tbl |>
  filter(site_id %in% bris_all_wide_tbl$site_id) |>
  group_by(site_id) |>
  summarise(sparkline = list(no2)) |>
  glimpse()


# make the wide table for GT display
bris_no2_wide_tbl <- inner_join(
  bris_all_wide_tbl,
  sparklines_tbl,
  by = join_by("site_id" == "site_id")
) |>
  glimpse()

dbDisconnect(con)

no2_table_gt <- bris_no2_wide_tbl |>
  mutate(across(-c(site_id, sparkline), ~ round(.x, 0))) |>
  gt() |>
  data_color(
    columns = 2:15,
    fn = \(x) {
      case_when(
        x < 40 ~ "#0da11c",
        x >= 40 & x < 60 ~ "red",
        x >= 60 ~ "purple"
      )
    },
    alpha = 0.7,
    apply_to = "fill"
  ) |>
  tab_style(
    style = list(
      # cell_fill(color = "white"),
      cell_text(weight = "bold")
    ),
    locations = cells_body(columns = starts_with("20"), rows = everything())
  ) |>
  gt_plt_sparkline(
    column = "sparkline",
    fig_dim = c(5, 20),
    label = FALSE
  ) |>
  tab_header(
    title = md("Bristol nitrogen dioxide diffusion tube data"),
    subtitle = "Roadside sites"
  ) |>
  tab_spanner(
    label = md("Annual mean NO~2~ concentrations"),
    columns = starts_with("20")
  ) |>
  cols_label(
    site_id = "Site ID",
    sparkline = "Trend"
  ) |>
  tab_footnote(
    footnote = "Sites where 2010 concentration >= 60 µg/m³",
    locations = cells_title(groups = "title")
  )

no2_table_gt
# save
no2_table_gt |>
  gtsave(
    filename = "bristol_no2_table.png",
    path = "plots/",
    zoom = 3,
    expand = 5,
    vwidth = 1900,
    vheight = 1200
  )
