library(tidyverse)
library(tidylog)

detention_stints <- arrow::read_feather(
  "data/detention-stints-latest.feather"
) |>
  as_tibble() |>
  filter(likely_duplicate == FALSE | is.na(likely_duplicate))

book_ins_count <-
  detention_stints |>
  count(
    detention_facility_code,
    date = as_date(book_in_date_time),
    name = "n_book_ins"
  )

book_outs_count <-
  detention_stints |>
  count(
    detention_facility_code,
    date = as_date(book_out_date_time),
    name = "n_book_outs"
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
  group_by(detention_facility_code, date) |>
  summarize(
    n_detained = n(),
    n_detained_at_midnight = sum(
      as.Date(book_in_date_time) <= date &
        (is.na(book_out_date_time) |
          as.Date(book_out_date_time) > date),
      na.rm = TRUE
    ),
    n_male = sum(gender == "Male", na.rm = TRUE),
    n_female = sum(gender == "Female", na.rm = TRUE),
    n_convicted_criminal = sum(book_in_criminality == "1 Convicted Criminal"),
    n_possibly_under_18 = sum(
      date < as.Date(str_c(birth_year + 18, "-01-01")),
      na.rm = TRUE
    ),
    .groups = "drop"
  )

facilities_daily_population <-
  daily_population_statistics |>
  left_join(
    book_ins_count,
    by = c(
      "detention_facility_code",
      "date"
    )
  ) |>
  left_join(
    book_outs_count,
    by = c(
      "detention_facility_code",
      "date"
    )
  ) |>
  mutate(
    n_book_ins = replace_na(n_book_ins, 0L),
    n_book_outs = replace_na(n_book_outs, 0L)
  ) |>
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
      n_detained = 0L,
      n_detained_at_midnight = 0L,
      n_book_ins = 0L,
      n_book_outs = 0L,
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
