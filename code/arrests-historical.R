# Source-file selection
# | df  | folder               | window                   | role    | reason                                                            |
# |-----|----------------------|--------------------------|---------|-------------------------------------------------------------------|
# | df2 | 2023_ICFO_42034      | 2011-10-01 .. 2023-09-23 | use     | identifier + 4 cols; full pre-2023-09-24 coverage                 |
# | df5 | November 2025 Release | 2023-09-24 .. 2025-10-16 | use     | identifier + 19 cols; most recent release for the late window     |
# | df1 | 2022-ICFO-22955      | 2015-10-01 .. 2024-08-04 | exclude | identifier stored as numeric in xlsx; Excel's 15-sig-digit limit  |
# |     |                      |                          |         | truncated original 19-digit IDs (collisions confirmed)            |
# | df3 | 120125               | 2023-09-01 .. 2025-10-16 | exclude | functionally identical to df5 (same rows, ids, NA pattern)        |
# | df4 | uwchr                | 2011-10-01 .. 2023-01-29 | exclude | no identifier; window fully covered by df2 which has one          |

# --- Packages ---
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)
library(stringdist)
library(arrow)
library(ggplot2)
library(data.table)
library(tidylog)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
# ROOT: ice/
# df1 <- list.files(
#     path = "inputs/arrests/2022-ICFO-22955",
#     pattern = "\\.xlsx$",
#     recursive = TRUE,
#     full.names = TRUE
#   ) |>
#   map_dfr(\(fp) {
#     excel_sheets(fp) |>
#       map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
#   }) |>
#   mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
#   mutate(source_file = "2022-ICFO-22955") |>
#   select(where(is_not_blank_or_redacted))

df2 <- list.files(
    path = "inputs/arrests/2023_ICFO_42034",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  map_dfr(\(fp) {
    excel_sheets(fp) |>
      map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
  }) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(source_file = "2023_ICFO_42034") |>
  select(where(is_not_blank_or_redacted))

# df3 <- list.files(
#     path = "inputs/arrests/120125",
#     pattern = "\\.xlsx$",
#     recursive = TRUE,
#     full.names = TRUE
#   ) |>
#   map_dfr(\(fp) {
#     excel_sheets(fp) |>
#       map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
#   }) |>
#   mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
#   mutate(source_file = "120125") |>
#   select(where(is_not_blank_or_redacted))

# df4 <- list.files(
#     path = "inputs/arrests/uwchr",
#     pattern = "\\.xlsx$",
#     recursive = TRUE,
#     full.names = TRUE
#   ) |>
#   map_dfr(\(fp) {
#     excel_sheets(fp) |>
#       map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
#   }) |>
#   mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
#   mutate(source_file = "uwchr") |>
#   select(where(is_not_blank_or_redacted))

df5 <- list.files(
    path = "inputs/arrests/November 2025 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  map_dfr(\(fp) {
    excel_sheets(fp) |>
      map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
  }) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(source_file = "November 2025 Release") |>
  select(where(is_not_blank_or_redacted))

# # --- Weekly counts for source-coverage check ---
# df1_weekly_counts <- get_weekly_counts(df1, "Apprehension_Date_And_Time")
# df2_weekly_counts <- get_weekly_counts(df2, "Apprehension_Date")
# df3_weekly_counts <- get_weekly_counts(df3, "Apprehension_Date")
# df4_weekly_counts <- get_weekly_counts(df4, "Arrest_Date")
# df5_weekly_counts <- get_weekly_counts(df5, "Apprehension_Date")

# all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts, df5_weekly_counts)

# ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
#   geom_line(alpha = 0.5)+
#   theme_minimal()

# boundaries <- all_weekly_counts |>
#   group_by(source_file) |>
#   summarise(start = min(week_start), end = max(week_start))

df2_trimmed <- df2 |>
  filter(Apprehension_Date < as.Date("2023-09-24"))
df5_trimmed <- df5 |>
  filter(Apprehension_Date >= as.Date("2023-09-24"))

# Free everything we no longer need before merging
rm(list = setdiff(ls(), c("df2_trimmed", "df5_trimmed")))
gc()

source("code/functions/safe_bind_rows.R")

arrests_df <- df2_trimmed |>
  rename(Unique_Identifier      = Anonymized_Identifier,
         Apprehension_Date_Time = Apprehension_Date) |>
  mutate(Apprehension_Date_Time = as.POSIXct(Apprehension_Date_Time, tz = "UTC")) |>
  safe_bind_rows(
    df5_trimmed |>
      rename(Apprehension_Date_Time = Apprehension_Date)
  ) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(apprehension_date = as.Date(apprehension_date_time))

rm(df2_trimmed, df5_trimmed)
gc()

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(arrests_df)
setorder(arrests_df, unique_identifier, apprehension_date_time)

arrests_df[,
  `:=`(
    hours_since_last = as.numeric(
      apprehension_date_time - shift(apprehension_date_time, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(apprehension_date_time, type = "lead") - apprehension_date_time,
      units = "hours"
    )
  ),
  by = unique_identifier
]

arrests_df <-
  arrests_df |>
  as_tibble() |>
  mutate(
    within_24hrs_prior = !is.na(hours_since_last) & hours_since_last <= 24,
    within_24hrs_next  = !is.na(hours_until_next) & hours_until_next <= 24,

    duplicate_likely = case_when(
      !is.na(unique_identifier) ~ within_24hrs_prior | within_24hrs_next,
      TRUE ~ FALSE
    ),

    # NEW: which rows to drop (drop the later row in a <=24hr pair)
    drop_row = case_when(
      is.na(unique_identifier) ~ FALSE,
      within_24hrs_prior ~ TRUE,   # later-than-previous within 24h => drop
      TRUE ~ FALSE
    )
  ) |>
  select(
    -within_24hrs_prior,
    -within_24hrs_next,
    -hours_since_last,
    -hours_until_next
  )

write_parquet(arrests_df, "data/arrests-historical.parquet", compression = "zstd", compression_level = 19)
