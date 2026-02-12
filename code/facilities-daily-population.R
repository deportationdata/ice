library(tidyverse)
library(tidylog)

detention_stints <- arrow::read_feather(
  "data/detention-stints-latest.feather"
) |>
  as_tibble() |>
  mutate(
    unique_identifier_nona = if_else(
      is.na(unique_identifier),
      str_c("noid_", row_number()),
      unique_identifier
    )
  )

daily_population <-
  detention_stints |>
  mutate(
    start_date = as_date(book_in_date_time),
    end_date = as_date(book_out_date_time),
    end_date = if_else(is.na(end_date), as_date("2025-10-15"), end_date)
  ) |>
  # ensure end_date is not before start_date (recode to start_date if so)
  mutate(end_date = pmax(start_date, end_date)) |>
  rowwise() |>
  mutate(date = list(seq(start_date, end_date, by = "day"))) |>
  unnest(date) |>
  ungroup()

daily_population_statistics <-
  daily_population |>
  mutate(
    detained_at_midnight = as.Date(book_in_date_time) <= as.Date(date) &
      (is.na(book_out_date_time) |
        as.Date(book_out_date_time) >= (as.Date(date) + 1))
  ) |>
  select(
    unique_identifier,
    unique_identifier_nona,
    detention_facility_code,
    date,
    gender,
    book_in_criminality,
    birth_year,
    detained_at_midnight
  ) |>
  group_by(detention_facility_code, date) |>
  summarize(
    n_detained = n_distinct(unique_identifier_nona, na.rm = TRUE),
    n_detained_at_midnight = n_distinct(
      unique_identifier_nona[detained_at_midnight],
      na.rm = TRUE
    ),
    n_male = n_distinct(
      unique_identifier_nona[gender == "Male"],
      na.rm = TRUE
    ),
    n_female = n_distinct(
      unique_identifier_nona[gender == "Female"],
      na.rm = TRUE
    ),
    n_convicted_criminal = n_distinct(
      unique_identifier_nona[book_in_criminality == "1 Convicted Criminal"],
      na.rm = TRUE
    ),
    n_possibly_under_18 = n_distinct(
      unique_identifier_nona[date < as.Date(str_c(birth_year + 18, "-01-01"))],
      na.rm = TRUE
    ),
    .groups = "drop"
  )

facilities_daily_population <-
  daily_population_statistics |>
  arrange(
    detention_facility_code,
    date
  ) |>
  filter(
    date >= as.Date("2023-09-01"),
    date <= as.Date("2025-10-15")
  ) |>
  complete(
    detention_facility_code,
    date = seq(
      as.Date("2023-09-01"),
      as.Date("2025-10-15"),
      by = "day"
    ),
    fill = list(
      n_missing_ID = 0L,
      n_detained = 0L,
      n_detained_at_midnight = 0L,
      n_male = 0L,
      n_female = 0L,
      n_convicted_criminal = 0L,
      n_possibly_under_18 = 0L
    )
  ) |>
  left_join(
    detention_stints |>
      select(detention_facility_code, detention_facility) |>
      distinct(),
    by = "detention_facility_code"
  ) |>
  relocate(n_book_ins, n_book_outs, .after = date) |>
  relocate(detention_facility, .after = detention_facility_code)

arrow::write_feather(
  facilities_daily_population,
  "data/facilities-daily-population.feather"
)
haven::write_dta(
  facilities_daily_population,
  "data/facilities-daily-population.dta"
)
haven::write_sav(
  facilities_daily_population,
  "data/facilities-daily-population.sav"
)

facilities_daily_population |>
  writexl::write_xlsx("data/facilities-daily-population.xlsx")
