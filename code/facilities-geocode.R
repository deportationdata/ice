library(tidyverse)
library(sf)
library(tidygeocoder)

facilities_details <- arrow::read_feather("data/facilities-details.feather")

facilities_geocoded <-
  facilities_details |>
  select(detention_facility_code, address, city, state, zip) |>
  distinct() |>
  mutate(full_address = glue::glue("{address}, {city}, {state} {zip}")) |>
  geocode(
    full_address,
    method = 'google',
    lat = latitude,
    long = longitude,
    limit = 1,
    full_results = TRUE
  )

facilities_geocoded_df <-
  facilities_geocoded |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    formatted_address,
    address_components
  ) |>
  unnest(address_components) |>
  unnest(types) |>
  group_by(detention_facility_code) |>
  mutate(
    is_exact = any(types %in% c("street_number", "premise"))
  ) |>
  ungroup() |>
  filter(
    types %in%
      c(
        "locality",
        "administrative_area_level_1",
        "administrative_area_level_2",
        "premise"
      )
  ) |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    formatted_address,
    types,
    short_name,
    is_exact
  ) |>
  pivot_wider(
    names_from = types,
    values_from = short_name
  ) |>
  mutate(
    # don't include facility name in the formatted address
    formatted_address = str_remove(
      formatted_address,
      glue::glue("{premise}, ")
    ),
    .keep = "unused"
  ) |>
  filter(is_exact == TRUE, !is.na(latitude), !is.na(longitude)) |>
  rename(
    state = administrative_area_level_1,
    county = administrative_area_level_2,
    city = locality
  ) |>
  select(-is_exact)

arrow::write_feather(
  facilities_geocoded_df,
  "data/facilities-geocoded.feather"
)
