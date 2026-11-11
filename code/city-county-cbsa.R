# This script takes arrest city/state key values mapped to US census cells in `city-match.R`.
# Values not yet matched directly to a county are assigned a county, then a CBSA and CSA.
# Results are exported as `city-county-cbsa-crosswalk.parquet` and joined with arrests in `arrests-latest.R`
#

library(tidyverse)

source("code/functions/normalize_city.R")
shp_url <- "https://github.com/deportationdata/us-shapefiles/raw/559841da212143b706678866b736883d48cf8d4f/data"
options(timeout = 1800) # the 100 MB partition file downloads at ~200 KB/s from GitHub LFS

# ==== References ==============================================================

counties <-
  arrow::read_parquet(
    str_c(shp_url, "/counties-sf.parquet"),
    col_select = c("geoid", "stusps", "name")
  ) |>
  left_join(state_names, by = "stusps") |>
  transmute(
    county_geoid = geoid,
    county_stusps = stusps,
    county_name = name,
    key_county = normalize_city(name),
    key_state
  )

county_cbsa <-
  arrow::read_parquet(
    str_c(shp_url, "/counties-sf.parquet"),
    col_select = c("geoid", "cbsa_geoid", "csa_geoid")
  ) |>
  filter(!is.na(cbsa_geoid)) |>
  rename(county_geoid = geoid) |>
  left_join(
    arrow::read_parquet(
      str_c(shp_url, "/cbsas-sf.parquet"),
      col_select = c("geoid", "name", "lsad")
    ) |>
      transmute(
        cbsa_geoid = geoid,
        cbsa_name = name,
        cbsa_type = recode_values(lsad, "M1" ~ "Metro", "M2" ~ "Micro")
      ),
    by = "cbsa_geoid"
  ) |>
  left_join(
    arrow::read_parquet(
      str_c(shp_url, "/csas-sf.parquet"),
      col_select = c("geoid", "name")
    ) |>
      select(csa_geoid = geoid, csa_name = name),
    by = "csa_geoid"
  ) |>
  relocate(csa_geoid, .before = csa_name)

stopifnot(
  !anyDuplicated(county_cbsa$county_geoid),
  !anyNA(county_cbsa$cbsa_name),
  !anyNA(county_cbsa$cbsa_type),
  all(county_cbsa$county_geoid %in% counties$county_geoid),
  identical(is.na(county_cbsa$csa_geoid), is.na(county_cbsa$csa_name))
)

# each cell's county (the plurality of its 2020 population for a place cell) and share;
# the few place cells with no block have none
cell_county <-
  arrow::read_parquet(
    str_c(shp_url, "/places-county-subdivisions-sf.parquet"),
    col_select = c("geoid", "county_geoid", "county_share")
  ) |>
  filter(!is.na(county_geoid)) |>
  rename(cell_county = county_geoid)

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
  )

stopifnot(!anyDuplicated(manual_references[c("key_city", "key_state")]))

manual <-
  manual_references |>
  left_join(counties, by = c("key_county", "key_state")) |>
  transmute(key_city, key_state, county_geoid, manual_listed = TRUE)

stopifnot(
  nrow(manual) == nrow(manual_references),
  all(is.na(manual_references$key_county) | !is.na(manual$county_geoid))
)

# ==== GNIS county attribute -> county =========================================

# a GNIS name's county where USGS files all its features under one county
gnis_county <-
  arrow::read_parquet(
    str_c(shp_url, "/gnis-places-county-subdivisions-xwalk.parquet"),
    col_select = c("stusps", "name", "county_geoid")
  ) |>
  left_join(state_names, by = "stusps") |>
  transmute(
    key_city = normalize_city(name),
    key_state,
    gnis_county = county_geoid
  ) |>
  summarise(
    gnis_county = if_else(
      n_distinct(gnis_county, na.rm = TRUE) == 1,
      first(gnis_county, na_rm = TRUE),
      NA_character_
    ),
    .by = c(key_city, key_state)
  ) |>
  filter(!is.na(gnis_county))

# ==== City -> county ==========================================================

county_crosswalk <-
  crosswalk |>
  left_join(cell_county, by = "geoid", relationship = "many-to-one") |>
  left_join(
    manual |> rename(manual_county = county_geoid),
    by = c("key_city", "key_state")
  ) |>
  left_join(
    gnis_county |>
      semi_join(
        filter(crosswalk, match_tier == "T5 gnis"),
        by = c("key_city", "key_state")
      ),
    by = c("key_city", "key_state")
  ) |>
  mutate(
    name_county = if_else(geo_level == "county", geoid, NA_character_),
    # a county is a city only where it is a cell (the island territories)
    county_only = geo_level == "county" & is.na(cell_county)
  ) |>
  mutate(
    subdivision_county = if_else(
      raw_level == "county_subdivision" &
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
      !is.na(subdivision_county) ~ subdivision_county,
      !is.na(gnis_county) & geo_level == "place" ~ gnis_county,
      !is.na(cell_county) ~ cell_county,
      !is.na(name_county) ~ name_county
    ),
    county_method = case_when(
      !is.na(manual_listed) ~ if_else(
        is.na(manual_county),
        NA_character_,
        "manual"
      ),
      !is.na(subdivision_county) ~ "subdivision",
      !is.na(gnis_county) & geo_level == "place" ~ "gnis point",
      !is.na(cell_county) ~ "cell",
      !is.na(name_county) ~ "county name"
    ),
    county_share = case_when(
      county_method == "cell" ~ county_share,
      !is.na(county_geoid) ~ 1
    ),
    city_geoid = if_else(county_only, NA_character_, geoid),
    city_level = if_else(county_only, NA_character_, geo_level),
    city_name = if_else(county_only, NA_character_, matched_name),
    match_tier,
    reason
  ) |>
  left_join(
    counties |> select(county_geoid, county_stusps, county_name),
    by = "county_geoid"
  ) |>
  left_join(county_cbsa, by = "county_geoid") |>
  relocate(county_name, county_stusps, .after = county_geoid) |>
  arrange(county_method, desc(n_rows))

stopifnot(
  !anyDuplicated(county_crosswalk[c("key_city", "key_state")]),
  identical(
    is.na(county_crosswalk$county_method),
    is.na(county_crosswalk$county_geoid)
  ),
  nrow(county_crosswalk) == nrow(crosswalk),
  identical(
    is.na(county_crosswalk$cbsa_geoid),
    is.na(county_crosswalk$cbsa_name)
  ),
  identical(
    is.na(county_crosswalk$csa_geoid),
    is.na(county_crosswalk$csa_name)
  ),
  all(is.na(county_crosswalk$cbsa_geoid) | !is.na(county_crosswalk$county_geoid)),
  all(
    is.na(county_crosswalk$county_geoid) |
      county_crosswalk$county_geoid %in% counties$county_geoid
  ),
  with(
    county_crosswalk,
    all(
      is.na(city_geoid) |
        city_level == "place" |
        county_geoid == str_sub(city_geoid, 1, 5)
    )
  )
)

arrow::write_parquet(county_crosswalk, "data/city-county-cbsa-crosswalk.parquet")

# ==== Counts ==================================================================

county_crosswalk |>
  filter(!is.na(county_geoid)) |>
  summarise(strings = n(), rows = sum(n_rows), .by = cbsa_type) |>
  mutate(pct_rows = round(100 * rows / sum(rows), 1)) |>
  print()

county_crosswalk |>
  summarise(strings = n(), rows = sum(n_rows), .by = county_method) |>
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
