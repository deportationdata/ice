# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/save_historical_outputs.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in source data ----
detention_stays <- arrow::read_parquet("data/detention-stays-latest.parquet")
arrests <- arrow::read_parquet("data/arrests-latest.parquet")
removals <- arrow::read_parquet("data/removals-historical.parquet")

# ---- Check: source data ----
detention_stays |>
  col_exists(
    c(
      stay_ID,
      unique_identifier,
      stay_book_in_date_time,
      stay_book_out_date_time,
      final_program
    )
  ) |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    stay_book_in_date_time,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

arrests |>
  col_exists(
    c(
      unique_identifier,
      apprehension_date_time,
      apprehension_method,
      apprehension_criminality,
      apprehension_aor,
      file_original,
      row_original,
      sheet_original
    )
  ) |>
  col_vals_not_null(
    apprehension_date_time,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

removals |>
  col_exists(c(unique_identifier, departed_date, departure_country)) |>
  col_vals_not_null(
    departed_date,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()


# ---- Build per-stay arrest candidate ----
detention_arrest_join <-
  detention_stays |>
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  left_join(
    arrests |>
      filter(!is.na(unique_identifier)) |>
      mutate(
        reprocessed_record = apprehension_method == "ERO Reprocessed Arrest"
      ) |>
      arrange(unique_identifier, reprocessed_record, apprehension_date_time) |>
      mutate(
        new_episode = is.na(lag(apprehension_date_time)) |
          difftime(
            apprehension_date_time,
            lag(apprehension_date_time),
            units = "hours"
          ) >
            24,
        episode_id = cumsum(new_episode),
        .by = "unique_identifier"
      ) |>
      filter(row_number() == 1, .by = c("unique_identifier", "episode_id")) |>
      select(-new_episode, -episode_id) |>
      select(
        unique_identifier,
        apprehension_date_time_arrest = apprehension_date_time,
        apprehension_criminality,
        apprehension_method,
        apprehension_aor,
        file_original_arrest = file_original,
        row_original_arrest = row_original,
        sheet_original_arrest = sheet_original
      ),
    by = "unique_identifier",
    relationship = "many-to-many"
  ) |>
  mutate(
    time_diff = as.numeric(difftime(
      stay_book_in_date_time,
      apprehension_date_time_arrest,
      units = "hours"
    ))
  ) |>
  group_by(stay_ID) |>
  filter(time_diff <= 24 * 10, time_diff >= 24 * -5) |>
  slice_min(order_by = abs(time_diff), n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(-time_diff, -stay_book_in_date_time, -unique_identifier)


# ---- Check: arrest candidate ----
detention_arrest_join |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    apprehension_date_time_arrest,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()


# ---- Build per-stay removal candidate ----
detention_removal_join <-
  detention_stays |>
  select(
    stay_ID,
    unique_identifier,
    stay_book_out_date_time,
    departed_date,
    departure_country
  ) |>
  mutate(
    departed_date_stay = departed_date,
    departure_country_stay = departure_country
  ) |>
  select(-departed_date, -departure_country) |>
  left_join(
    removals |>
      filter(!is.na(unique_identifier)) |>
      select(
        unique_identifier,
        departed_date_removal = departed_date,
        departure_country_removal = departure_country
      ),
    by = "unique_identifier",
    relationship = "many-to-many"
  ) |>
  pivot_longer(
    cols = c(departed_date_stay, departed_date_removal),
    names_to = "source",
    values_to = "departed_date_candidate",
    values_drop_na = TRUE
  ) |>
  mutate(
    departure_country = if_else(
      source == "departed_date_stay",
      departure_country_stay,
      departure_country_removal
    )
  ) |>
  select(-departure_country_stay, -departure_country_removal) |>
  mutate(
    time_diff = as.numeric(difftime(
      departed_date_candidate,
      stay_book_out_date_time,
      units = "days"
    ))
  ) |>
  filter(time_diff >= -5, time_diff <= 10) |>
  slice_min(
    order_by = abs(time_diff),
    n = 1,
    with_ties = FALSE,
    by = stay_ID
  ) |>
  select(
    stay_ID,
    departed_date_removal = departed_date_candidate,
    departure_country
  )


# ---- Check: removal candidate ----
detention_removal_join |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    departed_date_removal,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()


# ---- Tag border-vs-interior on stays ----
detention_stays <-
  detention_stays |>
  mutate(
    border_arrest = final_program %in%
      c(
        "Border Patrol",
        "Inspections - Air",
        "Inspections - Land",
        "Inspections - Sea",
        "Coast Guard"
      )
  )


# ---- Merge arrests and removals onto stays ----
n_stays_pre <- nrow(detention_stays)

detentions_with_arrests <-
  detention_stays |>
  select(-departed_date, -departure_country) |>
  left_join(
    detention_arrest_join |> mutate(has_arrest = 1L),
    by = "stay_ID",
    relationship = "one-to-one"
  ) |>
  left_join(
    detention_removal_join,
    by = "stay_ID",
    relationship = "one-to-one"
  ) |>
  mutate(
    has_arrest = if_else(is.na(has_arrest), 0L, has_arrest),
    has_removal = if_else(is.na(departed_date_removal), 0L, 1L)
  )

stopifnot(nrow(detentions_with_arrests) == n_stays_pre)


# ---- Final pointblank validation ----
detentions_with_arrests |>
  col_vals_expr(
    expr = expr(!if_any(where(is.character), is_redacted)),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    has_arrest,
    c(0L, 1L),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    has_removal,
    c(0L, 1L),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    border_arrest,
    c(TRUE, FALSE),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_arrest == 0L | !is.na(apprehension_date_time_arrest)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_arrest == 0L | !is.na(file_original_arrest)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(
      has_arrest == 0L |
        (as.numeric(difftime(
          stay_book_in_date_time,
          apprehension_date_time_arrest,
          units = "hours"
        )) >=
          -120 &
          as.numeric(difftime(
            stay_book_in_date_time,
            apprehension_date_time_arrest,
            units = "hours"
          )) <=
            240)
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_removal == 0L | !is.na(departure_country)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(
      has_removal == 0L |
        is.na(stay_book_out_date_time) |
        (as.numeric(difftime(
          departed_date_removal,
          stay_book_out_date_time,
          units = "days"
        )) >=
          -5 &
          as.numeric(difftime(
            departed_date_removal,
            stay_book_out_date_time,
            units = "days"
          )) <=
            10)
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_lte(
    has_arrest,
    1L,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_lte(
    has_removal,
    1L,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()


# ---- Reasonableness checks ----
match_rate_arrest <- mean(detentions_with_arrests$has_arrest == 1L)
match_rate_removal <- mean(detentions_with_arrests$has_removal == 1L)
message(sprintf(
  "Arrest match rate: %.1f%%   Removal match rate: %.1f%%",
  100 * match_rate_arrest,
  100 * match_rate_removal
))
if (match_rate_arrest < 0.30 || match_rate_arrest > 0.99) {
  warning("Arrest match rate outside expected band (30%-99%); inspect.")
}
if (match_rate_removal < 0.10 || match_rate_removal > 0.99) {
  warning("Removal match rate outside expected band (10%-99%); inspect.")
}


# ---- Sort and save ----
detentions_with_arrests <-
  detentions_with_arrests |>
  arrange(stay_book_in_date_time)

save_historical_outputs(
  detentions_with_arrests,
  "detention-stays-with-arrests-removals"
)
