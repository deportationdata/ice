library(sf)
library(tidyverse)

source("code/functions/normalize_city.R")

# ==== References ==============================================================

shp_dir <- "~/github/us-shapefiles/data"

source("code/functions/check_us_shapefiles.R")
check_us_shapefiles(shp_dir)
counties_sf <- st_read(
  file.path(shp_dir, "counties-sf.parquet"),
  quiet = TRUE
)

counties <-
  counties_sf |>
  st_drop_geometry() |>
  left_join(state_names, by = "stusps") |>
  transmute(
    county_geoid = geoid,
    county_stusps = stusps,
    county_name = name,
    key_county = normalize_city(name),
    key_state
  )

cells <-
  arrow::read_parquet(
    file.path(shp_dir, "places-cousubs-sf.parquet"),
    col_select = c("geoid", "stusps", "county_geoid")
  )

xwalk_cells <-
  arrow::read_parquet(
    file.path(shp_dir, "places-cousubs-xwalk.parquet"),
    col_select = c("geoid", "geo_level", "cell_geoid")
  ) |>
  filter(!is.na(cell_geoid))

crosswalk <-
  arrow::read_parquet("data/city-match-crosswalk.parquet")

manual_references <-
  readr::read_csv(
    "inputs/city-county-manual.csv",
    col_types = readr::cols(.default = "c")
  ) |>
  transmute(
    key_city = normalize_city(city),
    key_state = normalize_state(state),
    key_county = normalize_city(county)
  ) |>
  distinct(key_city, key_state, .keep_all = TRUE)

manual <-
  manual_references |>
  left_join(counties, by = c("key_county", "key_state")) |>
  transmute(key_city, key_state, county_geoid, manual_listed = TRUE)

stopifnot(
  nrow(manual) == nrow(manual_references),
  all(is.na(manual_references$key_county) | !is.na(manual$county_geoid))
)

# ==== Cell -> county ==========================================================

direct <-
  cells |>
  semi_join(counties, by = "county_geoid", na_matches = "never") |>
  transmute(cell_geoid = geoid, county_geoid, county_share = 1)

blocks <-
  arrow::read_parquet(
    file.path(shp_dir, "blocks-xwalk.parquet"),
    col_select = c("pop20", "aland", "county_geoid", "place_geoid", "cousub_geoid")
  ) |>
  left_join(
    xwalk_cells |>
      filter(geo_level == "place") |>
      select(place_geoid = geoid, place_cell = cell_geoid),
    by = "place_geoid"
  ) |>
  left_join(
    xwalk_cells |>
      filter(geo_level == "cousub") |>
      select(cousub_geoid = geoid, cousub_cell = cell_geoid),
    by = "cousub_geoid"
  ) |>
  transmute(
    cell_geoid = coalesce(place_cell, cousub_cell),
    county_geoid,
    pop20 = coalesce(pop20, 0L),
    aland
  ) |>
  filter(!is.na(cell_geoid), !cell_geoid %in% direct$cell_geoid) |>
  semi_join(counties, by = "county_geoid")

by_pop <-
  blocks |>
  summarise(pop = sum(pop20), aland = sum(aland), .by = c(cell_geoid, county_geoid)) |>
  mutate(
    county_share = if (sum(pop) > 0) pop / sum(pop) else aland / sum(aland),
    .by = cell_geoid
  ) |>
  slice_max(county_share, n = 1, by = cell_geoid, with_ties = FALSE) |>
  select(cell_geoid, county_geoid, county_share)

cell_county <-
  bind_rows(direct, by_pop) |>
  left_join(counties |> select(county_geoid, county_name), by = "county_geoid")

stopifnot(
  !anyDuplicated(cell_county$cell_geoid),
  all(cell_county$cell_geoid %in% cells$geoid),
  all(!is.na(cell_county$county_geoid)),
  all(is.finite(cell_county$county_share))
)

cat("cells without a county:", sum(!cells$geoid %in% cell_county$cell_geoid), "\n")

arrow::write_parquet(cell_county, "data/cell-county-xwalk.parquet")

# ==== GNIS points -> county ===================================================

gnis_county <-
  st_read(
    file.path(shp_dir, "gnis-populated-places-sf.parquet"),
    quiet = TRUE
  ) |>
  st_join(counties_sf |> select(gnis_county = geoid), join = st_within) |>
  st_drop_geometry() |>
  left_join(state_names, by = "stusps") |>
  transmute(
    key_city = normalize_city(name),
    key_state,
    gnis_county
  ) |>
  group_by(key_city, key_state) |>
  summarise(
    gnis_county = if_else(
      n_distinct(gnis_county, na.rm = TRUE) == 1,
      first(gnis_county, na_rm = TRUE),
      NA_character_
    ),
    .groups = "drop"
  ) |>
  filter(!is.na(gnis_county))

# ==== City -> county ==========================================================

county_crosswalk <-
  crosswalk |>
  left_join(
    cell_county |> rename(geoid = cell_geoid, cell_county = county_geoid),
    by = "geoid"
  ) |>
  left_join(
    manual |> rename(manual_county = county_geoid),
    by = c("key_city", "key_state")
  ) |>
  left_join(
    gnis_county |>
      semi_join(
        filter(crosswalk, match_tier == "T4 gnis"),
        by = c("key_city", "key_state")
      ),
    by = c("key_city", "key_state")
  ) |>
  mutate(name_county = if_else(geo_level == "county", geoid, NA_character_)) |>
  mutate(
    cousub_county = if_else(
      raw_level == "cousub" &
        str_sub(raw_geoid, 1, 5) %in% counties$county_geoid,
      str_sub(raw_geoid, 1, 5),
      NA_character_
    )
  ) |>
  transmute(
    key_city,
    key_state,
    n_rows,
    county_geoid = case_when(
      !is.na(manual_listed) ~ manual_county,
      !is.na(cousub_county) ~ cousub_county,
      !is.na(gnis_county) ~ gnis_county,
      !is.na(cell_county) ~ cell_county,
      !is.na(name_county) ~ name_county
    ),
    county_method = case_when(
      !is.na(manual_listed) ~ if_else(is.na(manual_county), NA_character_, "manual"),
      !is.na(cousub_county) ~ "subdivision",
      !is.na(gnis_county) ~ "gnis point",
      !is.na(cell_county) ~ "cell",
      !is.na(name_county) ~ "county name"
    ),
    county_share = case_when(
      county_method == "cell" ~ county_share,
      !is.na(county_geoid) ~ 1
    ),
    city_geoid = if_else(geo_level == "county", NA_character_, geoid),
    city_level = if_else(geo_level == "county", NA_character_, geo_level),
    city_name = if_else(geo_level == "county", NA_character_, matched_name),
    match_tier,
    reason
  ) |>
  left_join(
    counties |> select(county_geoid, county_stusps, county_name),
    by = "county_geoid"
  ) |>
  relocate(county_name, county_stusps, .after = county_geoid) |>
  arrange(county_method, desc(n_rows))

stopifnot(
  !anyDuplicated(county_crosswalk[c("key_city", "key_state")]),
  identical(
    is.na(county_crosswalk$county_method),
    is.na(county_crosswalk$county_geoid)
  ),
  nrow(county_crosswalk) == nrow(crosswalk),
  all(
    is.na(county_crosswalk$county_geoid) |
      county_crosswalk$county_geoid %in% counties$county_geoid
  )
)

arrow::write_parquet(county_crosswalk, "data/city-county-crosswalk.parquet")

# ==== Counts ==================================================================

county_crosswalk |>
  group_by(county_method) |>
  summarise(strings = n(), rows = sum(n_rows)) |>
  mutate(pct_rows = round(100 * rows / sum(rows), 1)) |>
  print(n = Inf)

county_crosswalk |>
  filter(county_method == "cell", county_share < 0.9) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, city_name, county_name, county_share) |>
  print(n = 25)

county_crosswalk |>
  filter(is.na(county_geoid), !is.na(key_city)) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, match_tier, reason) |>
  print(n = 25)
