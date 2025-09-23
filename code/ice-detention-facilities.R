library(tidyverse)
library(readxl)
library(tidygeocoder)

detentions_current <- arrow::read_feather("data/detentions-latest.feather")

detentions_2012_2023 <- arrow::read_feather(
  "data/ice-detentions-2012-2023.feather"
)

# bind detentions to obtain all facilities used since 2012
detentions_facilities <-
  bind_rows(
    detentions_2012_2023 |>
      select(
        detention_facility_code,
        detention_facility,
        detention_book_in_date,
        detention_book_out_date,
        anonymized_identifier,
        gender
      ),
    detentions_current |>
      select(
        detention_facility_code,
        detention_facility,
        detention_book_in_date = book_in_date_time,
        detention_book_out_date = detention_book_out_date_time,
        anonymized_identifier = unique_identifier,
        gender
      )
  ) |>
  group_by(detention_facility_code, detention_facility) |>
  summarize(
    first_used = as.Date(min(
      pmin(detention_book_out_date, detention_book_in_date, na.rm = TRUE),
      na.rm = TRUE
    )),
    last_used = as.Date(max(
      pmax(detention_book_out_date, detention_book_in_date, na.rm = TRUE),
      na.rm = TRUE
    )),
    n_stints = n(),
    n_stints_2025 = sum(detention_book_in_date >= "2025-01-01", na.rm = TRUE),
    n_individuals = n_distinct(anonymized_identifier),
    n_individuals_2025 = sum(
      detention_book_in_date >= "2025-01-01",
      na.rm = TRUE
    ),
    proportion_male = mean(
      gender == "Male" & gender != "Unknown",
      na.rm = TRUE
    ),
    .groups = "drop"
  )

# --- Bring in Vera data with addresses

# load Vera data
vera_facilities <- read_csv(
  "https://github.com/vera-institute/ice-detention-trends/raw/8c84c92f3029a6dbce08bfcf50ea6876d9af2eba/metadata/facilities.csv"
)

ice_detention_facilities <- detentions_facilities |>
  left_join(
    vera_facilities |> select(-detention_facility_name),
    by = "detention_facility_code"
  ) |>
  mutate(address = NA, uncertainty = NA) |>
  relocate(
    detention_facility_code,
    detention_facility,
    latitude,
    longitude,
    address,
    city,
    state,
    type_detailed,
    type_grouped) |>
  relocate(
    .after = uncertainty
  # ) |>
  # arrow::write_feather(
  #   "data/ice-detention-facilities.feather"
  )

# new_ice_detention_facilities <- ice_detention_facilities |>
#   filter(first_used > "2025-06-10") |>
#   filter(is.na(city)) |> # facilities without addresses
#   writexl::write_xlsx("code/experiments/facilities/new-ice-detention-facilities.xlsx") # manually input addresses and uncertainties

ice_detention_facilities <- ice_detention_facilities |>
  left_join(
    new_ice_detention_facilities |>
      select(detention_facility_code, address, city, state),
    by = "detention_facility_code"
  ) |>
  mutate(
    address = coalesce(address.x, address.y),
    city = coalesce(city.x, city.y),
    state = coalesce(state.x, state.y)
  ) |>
  select(-address.x, -address.y, -city.x, -city.y, -state.x, -state.y) |>
  relocate(
    address, city, state, .after = longitude
  ) |>
  writexl::write_xlsx ("code/experiments/facilities/ice-detention-facilities.xlsx")

geocoded_census <-
  ice_detention_facilities |>
  geocode(
    address = address,
    method = "census",
    lat = latitude_census,
    long = longitude_census
  )

not_geocoded_census <-
  ice_detention_facilities |>
  anti_join(
    geocoded_census |> filter(!is.na(latitude_census)),
    by = c("detention_facility_code")
  )

# Sys.setenv(GOOGLEGEOCODE_API_KEY = $GOOGLE_API_KEY)

geocoded_google <-
  not_geocoded_census |>
  geocode(
    address = address,
    method = "google",
    lat = latitude_google,
    long = longitude_google
  )

not_geocoded_google <-
  not_geocoded_census |>
  anti_join(
    geocoded_google |> filter(!is.na(latitude_google)),
    by = c("detention_facility_code")
  )

geocoded_osm <-
  not_geocoded_google |>
  geocode(
    address = address,
    method = "osm",
    lat = latitude_osm,
    long = longitude_osm
  )

all_geocoded <-
  bind_rows(
    "census" = geocoded_census |>
      select(
        detention_facility_code:uncertainty,
        address,
        latitude = latitude_census,
        longitude = longitude_census,
        -latitude, -longitude
      ),
    "google" = geocoded_google |>
      select(
        detention_facility_code:uncertainty,
        address,
        latitude = latitude_google,
        longitude = longitude_google,
        -latitude, -longitude
      ),
    "osm" = geocoded_osm |>
      select(
        detention_facility_code:uncertainty,
        address,
        latitude = latitude_osm,
        longitude = longitude_osm,
        -latitude, -longitude
      ),
    .id = "geocode_source"
  ) |>
  filter(!is.na(latitude)) |>
  relocate(
    latitude, longitude, .before = address
  )

# ungeocoded addresses
ungeocoded_addresses <-
  not_geocoded_google |>
  anti_join(
    geocoded_osm |> filter(!is.na(latitude_osm)),
    by = c("detention_facility_code")
  )