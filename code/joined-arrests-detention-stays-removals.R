# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/save_outputs.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in source data ----
joined_arrests_detention_stays <- arrow::read_parquet("data/joined-arrests-detention-stays-latest.parquet")
removals <- arrow::read_parquet("data/removals-latest.parquet")

# ---- Deduplicate removals: one per episode ----
removals_deduped <-
  removals |>
  filter(!duplicate_drop_row == TRUE) |>
  select(
    -duplicate_episode_count,
    -duplicate_episode_first,
    -duplicate_episode_last,
    -duplicate_likely
  ) |>
  mutate(removal_ID = row_number()) |>
  # flag all columns from removals table with "_removal"
  rename_with(~ str_c(.x, "_removal"), -c(removal_ID, unique_identifier))

# ---- Match stays to removals ----

stay_removal_pairs <-
  joined_arrests_detention_stays |>
  select(stay_ID, unique_identifier, stay_book_out_date_time) |>
  inner_join(
    removals_deduped |>
      filter(!is.na(unique_identifier)) |>
      select(removal_ID, unique_identifier, departed_date_removal),
    by = "unique_identifier",
    relationship = "many-to-many"
  ) |>
  mutate(
    time_diff = as.numeric(difftime(
      stay_book_out_date_time,
      departed_date_removal,
      units = "hours"
    ))
  ) |>
  # keep removals 5 days before to 10 days after the stay_book_out_date_time
  filter(time_diff <= 24 * 5, time_diff >= 24 * -10) |>
  # then keep the closest removal per book-out and the closest book-out per removal
  slice_min(
    order_by = abs(time_diff),
    n = 1,
    with_ties = FALSE,
    by = stay_ID
  ) |>
  slice_min(
    order_by = abs(time_diff),
    n = 1,
    with_ties = FALSE,
    by = removal_ID
  ) |>
  select(stay_ID, removal_ID)

# ---- Merge stays onto removals ----
n_pre <- nrow(joined_arrests_detention_stays)

joined_arrests_detention_stays_removals <-
  joined_arrests_detention_stays |>
  left_join(
    removals_deduped |>
      select(-unique_identifier) |>
      inner_join(stay_removal_pairs, by = "removal_ID"),
    by = "stay_ID",
    relationship = "one-to-one"
  ) |>
  mutate(has_matched_removal = !is.na(removal_ID))

stopifnot(nrow(joined_arrests_detention_stays_removals) == n_pre)

joined_arrests_detention_stays_removals |>
  arrow::write_parquet(here::here("data/joined-arrests-detention-stays-removals-diff.parquet"))

# Table now has cols from both joined and removals, need to select output cols without duplicates
# Where we match a removal, retain `departed_date`, `departure_country`, `port_of_departure` (any other cols?)
# Where we fail to match a removal but these cols are present, do we retain? Below retains

joined_arrests_detention_stays_removals_clean <- joined_arrests_detention_stays_removals |>
  mutate(
    departed_date = case_when(
      has_matched_removal == TRUE ~ departed_date_removal,
      TRUE ~ departed_date),
    departure_country = case_when(
      has_matched_removal == TRUE ~ departure_country_removal,
      TRUE ~ departure_country),
    port_of_departure = case_when(
      has_matched_removal == TRUE ~ port_of_departure_removal,
      TRUE ~ NA)
  ) |>
  dplyr::select(!ends_with("_removal") | has_matched_removal)

# What to do with stays that are not typed as removals but have removal info?
# How do we ensure that removals info agrees across stays, including those we matched and did not match?

joined_arrests_detention_stays_removals_clean |>
  mutate(is_removal = stay_release_reason %in%
    c("Removed",
    "Voluntary departure",
    "Voluntary Return",
    "Title 42 Return"),
    has_departed_date = !is.na(departed_date)) |>
  count(is_removal, has_matched_removal, has_departed_date)

# How would we reconstruct and validate "removal episodes"?
# A removal can't occur while someone is detained (except date/datetime overlap)