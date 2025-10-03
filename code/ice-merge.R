
library(tidyverse)
library(tidylog)

detentions <- arrow::read_feather("data/detentions-latest.feather")
detainers <- arrow::read_feather("data/detainers-latest.feather")
arrests <- arrow::read_feather("data/arrests-latest.feather")
encounters <- arrow::read_feather("data/encounters-latest.feather")

removals <- readxl::read_excel("~/dropbox/deportationdata/data/ICE/August 2025 Release/2025-ICLI-00019_2024-ICFO-39357_ICE Removals_LESA-STU_FINAL Redacted.xlsx", skip = 6) |>
  janitor::clean_names(allow_dupes = FALSE) |> 
  mutate(
    file = "2025-ICLI-00019_2024-ICFO-39357_ICE Removals.xlsx",
    sheet = "Removals",
    row = as.integer(row_number() + 6 + 1),
    departed_date = as.Date(departed_date)
  )

detentions_individual <- 
  detentions |> 
  group_by(unique_identifier) |>
  slice_max(order_by = stay_book_in_date_time, n = 1, with_ties = FALSE) |> 
  ungroup() |> 
  transmute(stay_ID, unique_identifier, stay_book_in_date_time, across(c(file, sheet, row), .names = "{.col}_detention"))

detentions_arrests_join <- 
  detentions_individual |> 
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  filter(!is.na(unique_identifier)) |>
  left_join(arrests |> filter(!is.na(unique_identifier)) |> select(unique_identifier, apprehension_date_arrest = apprehension_date, file_arrest = file, row_arrest = row, sheet_arrest = sheet), by = "unique_identifier", relationship = "many-to-many") |> 
  mutate(time_diff = as.numeric(difftime(stay_book_in_date_time, apprehension_date_arrest, units = "hours"))) |>
  group_by(stay_ID) |> 
  filter(time_diff <= 24*5, time_diff >= 0) |>
  slice_min(order_by = abs(time_diff), n = 1, with_ties = FALSE) |>
  ungroup() |> 
  select(-time_diff, -unique_identifier, -stay_book_in_date_time)

# join detentions to detainers get most recent detainer before detention
detentions_detainers_join <- 
  detentions_individual |> 
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  filter(!is.na(unique_identifier)) |>
  left_join(
    detainers |> filter(!is.na(unique_identifier)) |> select(unique_identifier, detainer_prepare_date_detainer = detainer_prepare_date, file_detainer = file, row_detainer = row, sheet_detainer = sheet),
    by = "unique_identifier",
    relationship = "one-to-many"
  ) |> 
  mutate(time_diff = as.numeric(difftime(as.Date(stay_book_in_date_time), detainer_prepare_date_detainer, units = "days"))) |>
  group_by(stay_ID) |> 
  filter(time_diff >= 0) |>
  slice_max(order_by = detainer_prepare_date_detainer, n = 1, with_ties = FALSE) |>
  ungroup() |> 
  select(-unique_identifier, -stay_book_in_date_time)

# join encounters to detentions get most recent encounter before detention
detentions_encounters_join <- 
  detentions_individual |> 
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  filter(!is.na(unique_identifier)) |>
  left_join(
    encounters |> filter(!is.na(unique_identifier)) |> select(unique_identifier, event_date_encounters = event_date, file_encounter = file, row_encounter = row, sheet_encounter = sheet),
    by = "unique_identifier",
    relationship = "one-to-many"
  ) |> 
  mutate(time_diff = as.numeric(difftime(as.Date(stay_book_in_date_time), event_date_encounters, units = "days"))) |>
  group_by(stay_ID) |> 
  filter(time_diff >= 0) |>
  slice_min(order_by = abs(time_diff), n = 1, with_ties = FALSE) |>
  ungroup() |> 
  select(-time_diff, -unique_identifier, -stay_book_in_date_time)

# join removals to detentions next removal after detention
detentions_removals_join <- 
  detentions_individual |> 
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  filter(!is.na(unique_identifier)) |>
  left_join(
    removals |> filter(!is.na(unique_identifier)) |> select(unique_identifier, departed_date_removal = departed_date, file_removal = file, row_removal = row, sheet_removal = sheet),
    by = "unique_identifier",
    relationship = "one-to-many"
  ) |> 
  mutate(time_diff = as.numeric(difftime(as.Date(stay_book_in_date_time), as.Date(departed_date_removal), units = "days"))) |>
  group_by(stay_ID) |> 
  filter(time_diff < 0) |>
  slice_min(order_by = desc(time_diff), n = 1, with_ties = FALSE) |>
  ungroup() |> 
  select(-time_diff, -unique_identifier, -stay_book_in_date_time)

enforcement_tbl <- 
  detentions_individual |> 
  left_join(detentions_arrests_join |> mutate(has_arrest = 1), by = "stay_ID", relationship = "one-to-one") |> 
  mutate(has_arrest = if_else(is.na(has_arrest), 0L, has_arrest)) |> 
  left_join(detentions_detainers_join |> mutate(has_detainer = 1), by = "stay_ID", relationship = "one-to-one") |> 
  mutate(has_detainer = if_else(is.na(has_detainer), 0L, has_detainer)) |> 
  left_join(detentions_encounters_join |> mutate(has_encounter = 1), by = "stay_ID", relationship = "one-to-one") |> 
  mutate(has_encounter = if_else(is.na(has_encounter), 0L, has_encounter)) |> 
  left_join(detentions_removals_join |> mutate(has_removal = 1), by = "stay_ID", relationship = "one-to-one") |> 
  mutate(has_removal = if_else(is.na(has_removal), 0L, has_removal))

arrow::write_feather(enforcement_tbl, "data/ice-enforcement-merge.feather")
