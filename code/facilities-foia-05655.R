library(tidyverse)
library(tidylog)

detentions_05655 <-
  list.files("~/Downloads/2024-ICFO-05655 (4)/", full.names = TRUE) |>
  set_names() |>
  map_dfr(~ readxl::read_excel(.x, skip = 4), .id = "file")

detentions_05655_df <-
  detentions_05655 |>
  janitor::clean_names() |>
  rename(
    detention_facility_operator = detention_facility_type_4,
    detention_facility_type = detention_facility_type_17
  ) |>
  select(
    c(contains("detention_facility"), -initial_book_in_detention_facility_code),
    detention_book_out_date_time
  ) |>
  mutate(
    detention_facility_operator = case_when(
      detention_facility_operator == "INS Facility" ~ "ICE Facility",
      TRUE ~ detention_facility_operator
    )
  ) |>
  slice_max(
    n = 1,
    by = detention_facility_code,
    order_by = detention_book_out_date_time,
    with_ties = FALSE
  ) |>
  relocate(detention_facility_code, detention_facility) |>
  select(-detention_book_out_date_time)

arrow::write_feather(
  detentions_05655_df,
  "data/facilities-foia-05655.feather"
)
