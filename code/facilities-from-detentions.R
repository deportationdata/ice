library(tidyverse)
library(readxl)
library(tidylog)

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

arrow::write_feather(
  detentions_facilities,
  "data/facilities-from-detentions.feather"
)

# # --- Bring in Vera data with addresses

# # load Vera data
# vera_facilities <- read_csv(
#   "https://github.com/vera-institute/ice-detention-trends/raw/8c84c92f3029a6dbce08bfcf50ea6876d9af2eba/metadata/facilities.csv"
# )

# new_facility_locations <- read_delim("inputs/facility-locations-oct2025.txt")

# facility_locations <-
#   bind_rows(
#     "vera" = vera_facilities,
#     "ddp" = new_facility_locations,
#     .id = "source"
#   )

# ice_detention_facilities <-
#   detentions_facilities |>
#   left_join(
#     facility_locations |> select(-detention_facility_name),
#     by = "detention_facility_code"
#   ) |>
#   relocate(
#     detention_facility_code,
#     detention_facility,
#     latitude,
#     longitude,
#     # address,
#     city,
#     state,
#     type_detailed,
#     type_grouped
#   )
