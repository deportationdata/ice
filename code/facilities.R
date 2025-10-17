library(tidyverse)
library(tidylog)

facilities_details <- arrow::read_feather("data/facilities-details.feather")
facilities_geocoded <- arrow::read_feather("data/facilities-geocoded.feather")

facilities_df <-
  facilities_details |>
  select(-address, -city, -state, -zip) |>
  left_join(facilities_geocoded, by = "detention_facility_code")

arrow::write_feather(facilities_df, "data/facilities.feather")
