# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Read in files ----
arrests_df <-
  arrow::read_parquet("data/intermediate/arrests-latest-read.parquet")

# ---- City geography ----
city_crosswalk <-
  arrow::read_parquet("data/city-match-crosswalk.parquet")

county_crosswalk <-
  arrow::read_parquet("data/city-county-crosswalk.parquet")

arrests_df <-
  arrests_df |>
  left_join(
    city_crosswalk |>
      filter(!is.na(key_city)) |>
      select(
        key_city,
        key_state,
        apprehension_city_geoid = geoid,
        apprehension_city_geo_level = geo_level,
        apprehension_city_name_census = matched_name,
        city_match_tier = match_tier
      ),
    by = c("key_city", "key_state"),
    relationship = "many-to-one",
    na_matches = "never"
  ) |>
  left_join(
    county_crosswalk |>
      filter(!is.na(key_city)) |>
      select(
        key_city,
        key_state,
        apprehension_county_geoid = county_geoid,
        apprehension_county_name_census = county_name,
        apprehension_county_method = county_method,
        apprehension_county_share = county_share
      ),
    by = c("key_city", "key_state"),
    relationship = "many-to-one",
    na_matches = "never"
  )

# ---- Check: city geography ----
arrests_df |>
  filter(!is.na(key_city), city_match_tier != "T0 triage") |>
  col_vals_not_null(
    apprehension_city_geoid,
    actions = action_levels(warn_at = 0.02, stop_at = 0.03)
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
