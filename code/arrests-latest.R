# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Read in files ----
arrests_df <-
  arrow::read_parquet("data/intermediate/arrests-latest-read.parquet")

# ---- City geography ----
# city-county-cbsa.R blanks the city columns for a county-only match (T4, or a
# manual alias to a county), so the city fields hold a county only where the
# county is itself the cell (the island territories).
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
        apprehension_city_name = city_name,
        apprehension_city_name_census = city_name_census,
        apprehension_city_share = city_share,
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
# unresolved (T6) rows stay under 3% of rows with a usable city string;
# strings absent from the crosswalk are not counted
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

arrests_df <-
  arrests_df |>
  select(-key_city, -key_state, -city_match_tier) |>
  relocate(
    apprehension_city_geoid,
    apprehension_city_geo_level,
    apprehension_city_name,
    apprehension_city_name_census,
    apprehension_city_share,
    apprehension_county_geoid,
    apprehension_county_name_census,
    apprehension_county_method,
    apprehension_county_share,
    apprehension_core_based_statistical_area,
    apprehension_core_based_statistical_area_code,
    apprehension_core_based_statistical_area_type,
    apprehension_combined_statistical_area,
    apprehension_combined_statistical_area_code,
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
