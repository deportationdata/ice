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

# ---- Deduplicate arrests: one per episode ----
arrests_deduped <-
  arrests |>
  filter(duplicate_episode_first) |>
  mutate(arrest_ID = row_number()) |>
  rename_with(~ str_c(.x, "_arrest"), -c(arrest_ID, unique_identifier))

# ---- Match stays to arrests ----
stay_arrest_pairs <-
  detention_stays |>
  select(stay_ID, unique_identifier, stay_book_in_date_time) |>
  inner_join(
    arrests_deduped |>
      filter(!is.na(unique_identifier)) |>
      select(arrest_ID, unique_identifier, apprehension_date_time_arrest),
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
  # then keep the closest arrest per stay and the closest stay per arrest
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
    by = arrest_ID
  ) |>
  select(stay_ID, arrest_ID)

# ---- Check: stay-arrest pairs ----
stay_arrest_pairs |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  rows_distinct(
    arrest_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    arrest_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# ---- Merge stays onto arrests ----
n_arrests_pre <- nrow(arrests_deduped)

arrests_with_detentions <-
  arrests_deduped |>
  left_join(
    detention_stays |>
      select(-unique_identifier) |>
      inner_join(stay_arrest_pairs, by = "stay_ID"),
    by = "arrest_ID",
    relationship = "one-to-one"
  ) |>
  mutate(has_detention_stay = !is.na(stay_ID))

stopifnot(nrow(arrests_with_detentions) == n_arrests_pre)

# ---- Fill shared variables from stays if detained, from arrests otherwise ----
shared_vars <- c(
  "final_program",
  "case_status",
  "case_category",
  "departure_country",
  "final_order_yes_no",
  "birth_year",
  "citizenship_country",
  "gender",
  "departed_date",
  "final_order_date",
  "case_threat_level"
)

arrests_with_detentions <-
  arrests_with_detentions |>
  mutate(departed_date_arrest = as_date(departed_date_arrest)) |>
  mutate(across(
    all_of(shared_vars),
    ~ if_else(has_detention_stay, .x, get(str_c(cur_column(), "_arrest")))
  )) |>
  select(-all_of(str_c(shared_vars, "_arrest")), -arrest_ID) |>
  rename_with(~ str_remove(.x, "_arrest$"))

# ---- Final pointblank validation ----
arrests_with_detentions |>
  col_vals_expr(
    expr = expr(!if_any(where(is.character), is_redacted)),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  rows_distinct(
    stay_ID,
    preconditions = ~ . %>% filter(!is.na(stay_ID)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    apprehension_date_time,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    has_detention_stay,
    c(FALSE, TRUE),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_detention_stay == FALSE | !is.na(stay_ID)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(has_detention_stay == FALSE | !is.na(stay_book_in_date_time)),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(
      has_detention_stay == FALSE |
        (as.numeric(difftime(
          stay_book_in_date_time,
          apprehension_date_time,
          units = "hours"
        )) >=
          -120 &
          as.numeric(difftime(
            stay_book_in_date_time,
            apprehension_date_time,
            units = "hours"
          )) <=
            240)
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

# ---- Reasonableness checks ----
match_rate_detention <- mean(arrests_with_detentions$has_detention_stay)
message(sprintf(
  "Detention stay match rate: %.1f%%",
  100 * match_rate_detention
))
if (match_rate_detention < 0.20 || match_rate_detention > 0.95) {
  warning("Detention match rate outside expected band (20%-95%); inspect.")
}

# ---- Sort and save ----
arrests_with_detentions <-
  arrests_with_detentions |>
  arrange(apprehension_date_time)

save_outputs(
  arrests_with_detentions,
  "joined-arrests-detention-stays"
)
