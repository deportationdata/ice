library(tidyverse)
library(tidygeocoder)

# reminder to set api key sysenv

vera_facilities <- read_csv(
  "https://github.com/vera-institute/ice-detention-trends/raw/8c84c92f3029a6dbce08bfcf50ea6876d9af2eba/metadata/facilities.csv"
)

# # check for missing coordinates
# missing_coords <-
#   vera_facilities |>
#   filter(is.na(latitude) | is.na(longitude))
# missing_coords # 2 missing observations

vera_reverse_geocoded <-
  vera_facilities |>
  reverse_geocode(
    lat = latitude,
    long = longitude,
    method = "osm",
    address = "address",
    progress_bar = TRUE,
    full_results = TRUE
  )

vera_reverse_geocoded |>
  mutate(
    final_city = coalesce(city...24, city...5),
    final_state = coalesce(state...26, state...6)
  ) |>
  mutate(across(
    any_of(c("address", "final_city", "final_state")),
    clean_text
  ))
