# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/save_outputs.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in source data ----
detention_stays <- arrow::read_parquet("data/detention-stays-latest.parquet")
arrests <- arrow::read_parquet("data/arrests-latest.parquet")

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
      duplicate_episode_first,
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

# ---- Build per-stay arrest candidate ----
arrests_first_per_episode <-
  arrests |>
  filter(!is.na(unique_identifier), duplicate_episode_first) |>
  select(
    -final_program,
    -case_status,
    -case_category,
    -departure_country,
    -final_order_yes_no,
    -birth_year,
    -citizenship_country,
    -gender,
    -departed_date,
    -final_order_date
  ) |>
  rename_with(~ str_c(.x, "_arrest"), -unique_identifier)

detention_arrest_join <-
  detention_stays |>
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  left_join(
    arrests_first_per_episode,
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
  # keep arrests 5 days before to 10 days after the stay_book_in_date_time
  filter(time_diff <= 24 * 10, time_diff >= 24 * -5) |>
  # then keep the closest one when there are multiple candidates per stay
  slice_min(
    order_by = abs(time_diff),
    n = 1,
    with_ties = FALSE,
    by = stay_ID
  ) |>
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

# ---- Merge arrests onto stays ----
n_stays_pre <- nrow(detention_stays)

detentions_with_arrests <-
  detention_stays |>
  select(-departed_date, -departure_country) |>
  left_join(
    detention_arrest_join |> mutate(has_arrest = TRUE),
    by = "stay_ID",
    relationship = "one-to-one"
  ) |>
  mutate(
    has_arrest = if_else(is.na(has_arrest), FALSE, has_arrest)
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
    c(FALSE, TRUE),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_arrest == FALSE | !is.na(apprehension_date_time_arrest)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_arrest == FALSE | !is.na(file_original_arrest)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(
      has_arrest == FALSE |
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
  col_vals_lte(
    has_arrest,
    TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

# ---- Reasonableness checks ----
match_rate_arrest <- mean(detentions_with_arrests$has_arrest == TRUE)
message(sprintf(
  "Arrest match rate: %.1f%%",
  100 * match_rate_arrest
))
if (match_rate_arrest < 0.30 || match_rate_arrest > 0.99) {
  warning("Arrest match rate outside expected band (30%-99%); inspect.")
}

# ---- Sort and save ----
detentions_with_arrests <-
  detentions_with_arrests |>
  arrange(stay_book_in_date_time)

save_outputs(
  detentions_with_arrests,
  "joined-arrests-detention-stays"
)
