# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/normalize_city.R")

shp_url <- "https://github.com/deportationdata/us-shapefiles/raw/559841da212143b706678866b736883d48cf8d4f/data"

# ---- Read in files ----
arrests_df <-
  arrow::read_parquet("data/intermediate/arrests-latest-read.parquet")

# ---- City geography ----
# city-county-cbsa.R carries the city columns with a county-only match (T4, or a
# manual alias to a county such as Louisville) blanked, so the city fields
# never hold a county.
county_crosswalk <-
  arrow::read_parquet("data/city-county-cbsa-crosswalk.parquet")

arrests_df <-
  arrests_df |>
  left_join(
    county_crosswalk |>
      filter(!is.na(key_city)) |>
      select(
        key_city,
        key_state,
        apprehension_city_geoid = city_geoid,
        apprehension_city_geo_level = city_level,
        apprehension_city_name_census = city_name,
        city_match_tier = match_tier,
        apprehension_county_geoid = county_geoid,
        apprehension_county_name_census = county_name,
        apprehension_county_method = county_method,
        apprehension_county_share = county_share,
        apprehension_core_based_statistical_area = cbsa_name,
        apprehension_core_based_statistical_area_code = cbsa_geoid,
        apprehension_core_based_statistical_area_type = cbsa_type,
        apprehension_combined_statistical_area = csa_name,
        apprehension_combined_statistical_area_code = csa_geoid
      ),
    by = c("key_city", "key_state"),
    relationship = "many-to-one",
    na_matches = "never"
  )

# ---- Check: city geography ----
# every matched string has a city, except the county-only matches
arrests_df |>
  filter(
    !is.na(key_city),
    city_match_tier != "T0 triage",
    !apprehension_county_method %in% "county name"
  ) |>
  col_vals_not_null(
    apprehension_city_geoid,
    actions = action_levels(warn_at = 0.02, stop_at = 0.03)
  ) |>
  col_vals_expr(
    expr(
      is.na(apprehension_core_based_statistical_area_code) ==
        is.na(apprehension_core_based_statistical_area)
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

# ---- 287(g) agreements ----
# active 287(g) agencies, matched to arrests at the agency's own jurisdiction
#   level (county, municipality, state)
agencies_287g <-
  arrow::read_parquet(
    "https://media.githubusercontent.com/media/deportationdata/ice-287g/main/data/agencies.parquet"
  ) |>
  filter(has_active)

counties_287g <-
  agencies_287g |>
  filter(jurisdiction_level == "County") |>
  distinct(apprehension_county_geoid = county_fips) |>
  mutate(apprehension_county_has_287g_agreement = TRUE)

municipalities_287g <-
  agencies_287g |>
  filter(jurisdiction_level == "Municipal", !is.na(place_geoid)) |>
  distinct(apprehension_city_geoid = place_geoid) |>
  mutate(apprehension_municipality_has_287g_agreement = TRUE)

states_287g <-
  agencies_287g |>
  filter(jurisdiction_level == "State") |>
  distinct(apprehension_state_geoid = state_fips) |>
  mutate(apprehension_state_has_287g_agreement = TRUE)

# state FIPS code (geoid) for each normalized state name
state_geoids <-
  arrow::read_parquet(
    str_c(shp_url, "/states-sf.parquet"),
    col_select = c("geoid", "stusps")
  ) |>
  left_join(state_names, by = "stusps") |>
  transmute(key_state, apprehension_state_geoid = geoid)

n_before_287g_join <- nrow(arrests_df)
arrests_df <-
  arrests_df |>
  left_join(
    state_geoids,
    by = "key_state",
    relationship = "many-to-one",
    na_matches = "never"
  ) |>
  left_join(
    counties_287g,
    by = "apprehension_county_geoid",
    relationship = "many-to-one",
    na_matches = "never"
  ) |>
  left_join(
    municipalities_287g,
    by = "apprehension_city_geoid",
    relationship = "many-to-one",
    na_matches = "never"
  ) |>
  left_join(
    states_287g,
    by = "apprehension_state_geoid",
    relationship = "many-to-one",
    na_matches = "never"
  ) |>
  # FALSE if the geography is known but has no agreement, NA if it is unknown
  mutate(
    apprehension_county_has_287g_agreement = if_else(
      is.na(apprehension_county_geoid),
      NA,
      coalesce(apprehension_county_has_287g_agreement, FALSE)
    ),
    apprehension_municipality_has_287g_agreement = if_else(
      is.na(apprehension_city_geoid),
      NA,
      coalesce(apprehension_municipality_has_287g_agreement, FALSE)
    ),
    apprehension_state_has_287g_agreement = if_else(
      is.na(apprehension_state_geoid),
      NA,
      coalesce(apprehension_state_has_287g_agreement, FALSE)
    )
  ) |>
  select(-apprehension_state_geoid)
stopifnot(nrow(arrests_df) == n_before_287g_join)

# ---- Check: 287(g) agreements ----
arrests_df |>
  col_exists(
    c(
      apprehension_county_has_287g_agreement,
      apprehension_municipality_has_287g_agreement,
      apprehension_state_has_287g_agreement
    )
  ) |>
  col_vals_in_set(
    apprehension_county_has_287g_agreement,
    c(TRUE, FALSE, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    apprehension_municipality_has_287g_agreement,
    c(TRUE, FALSE, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    apprehension_state_has_287g_agreement,
    c(TRUE, FALSE, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

arrests_df <-
  arrests_df |>
  select(-key_city, -key_state, -city_match_tier) |>
  relocate(
    apprehension_city_geoid,
    apprehension_city_geo_level,
    apprehension_city_name_census,
    apprehension_county_geoid,
    apprehension_county_name_census,
    apprehension_county_method,
    apprehension_county_share,
    apprehension_core_based_statistical_area,
    apprehension_core_based_statistical_area_code,
    apprehension_core_based_statistical_area_type,
    apprehension_combined_statistical_area,
    apprehension_combined_statistical_area_code,
    apprehension_county_has_287g_agreement,
    apprehension_municipality_has_287g_agreement,
    apprehension_state_has_287g_agreement,
    .after = apprehension_city_common_case
  )

# ---- Save Outputs ----
source("code/functions/save_outputs.R")
save_outputs(arrests_df, "arrests-latest")

arrests_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~ str_c("Arrests (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/arrests-latest.xlsx")

# END.
