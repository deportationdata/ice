# Source-file selection
# | df  | folder           | window                   | role           | reason                                |
# |-----|------------------|--------------------------|----------------|---------------------------------------|
# | df4 | 14-03290         | < 2013-09-29             | use            | only source with pre-2013 coverage    |
# | df1 | 2023_ICFO_42034  | 2013-09-29 .. 2023-09-23 | use            | mid-window coverage                   |
# | df2 | March 2026 Release | >= 2023-09-24          | use            | most recent release                   |
# | df3 | uwchr            | (whole)                  | loaded, unused | not used downstream                   |

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
library(pointblank)
library(haven)

# --- Source Functions ---
source("code/functions/process_folder_data.R")
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/if_has.R")
source("code/functions/coalesce_rename.R")
source("code/functions/save_historical_outputs.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
# ROOT: ice/
df1 <- list.files(
    path = "inputs/removals/2023_ICFO_42034",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000, force_col_type = "text"),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  convert_df_temporal_columns() |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(ends_with("_Date"), as.Date))

df2 <- list.files(
    path = "inputs/removals/March 2026 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000, force_col_type = "text"),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  convert_df_temporal_columns() |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(ends_with("_Date"), as.Date))

df3 <- list.files(
    path = "inputs/removals/uwchr",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000, force_col_type = "text"),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  convert_df_temporal_columns() |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(ends_with("_Date"), as.Date))

# --- df4 needs separate processing: read sheet 2 only from each file ---
df4 <- list.files(
    path = "inputs/removals/14-03290",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) {
      sh <- excel_sheets(fp)[2]
      process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000) |>
        mutate(sheet_original = sh)
    },
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  convert_df_temporal_columns() |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(ends_with("_Date"), as.Date))

# # --- Weekly counts for source-coverage check ---
# df1_weekly_counts <- get_weekly_counts(df1, "Departure_Date")
# df2_weekly_counts <- get_weekly_counts(df2, "Departed_Date")
# df3_weekly_counts <- get_weekly_counts(df3, "Departed_Date")
# df4_weekly_counts <- get_weekly_counts(df4, "Departed_Date")

# all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts)

# ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
#   geom_line(alpha = 0.5)+
#   theme_minimal()

# boundaries <- all_weekly_counts |>
#   group_by(source_file) |>
#   summarise(start = min(week_start), end = max(week_start))

# trim datasets by date and merge (14-03290, 2024_ICFO_42034, 082025)
df4_trimmed <- df4 |>
  filter(Departed_Date < as.Date("2013-09-29"))
df1_trimmed <- df1 |>
  filter(Departure_Date >= as.Date("2013-09-29") & Departure_Date < as.Date("2023-09-24"))
df2_trimmed <- df2 |>
  filter(Departed_Date >= as.Date("2023-09-24"))

# Drop the untrimmed originals and df3 (not used downstream)
rm(df1, df2, df3, df4)
gc()

source("code/functions/safe_bind_rows.R")

df12 <- df1_trimmed |>
  coalesce_rename("Departed_Date", "Departure_Date") |>
  coalesce_rename("Unique_Identifier", "Anonymized_Identifier") |>
  coalesce_rename("Unique_Identifier", "Anonymized_Identifer") |>
  safe_bind_rows(df2_trimmed)

rm(df1_trimmed, df2_trimmed)
gc()

removals_df <- df12 |>
  coalesce_rename("Latest_Arrest_Program", "Latest_Arrest_Program_Current") |>
  coalesce_rename("Latest_Arrest_Program_Code", "Latest_Arrest_Program_Current_Code") |>
  coalesce_rename("Latest_Arrest_Apprehension_Date", "Latest_Person_Apprehension_Date") |>
  coalesce_rename("Most_Recent_Prior_Depart_Date", "Latest_Person_Departed_Date") |>
  safe_bind_rows(
    df4_trimmed |>
      coalesce_rename("Port_of_Departure",   "Port_Of_Departure") |>
      coalesce_rename("Departure_Country",   "Departed_To_Country") |>
      coalesce_rename("Birth_Country",       "Country_of_Birth") |>
      coalesce_rename("Citizenship_Country", "Country_of_Citizenship") |>
      coalesce_rename("Birth_Year",          "Year_of_Birth") |>
      coalesce_rename("Case_Threat_Level",   "Rc_Threat_Level") |>
      coalesce_rename("Unique_Identifier",   "Unique_ID")
  ) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(departed_date = as.Date(departed_date)) |>
  redact_to_na() |>
  mutate(across(any_of("birth_year"), as.integer))

rm(df12, df4_trimmed)
gc()

# FLAG Duplicates
setDT(removals_df)
setorder(removals_df, unique_identifier, departed_date)

removals_df[,
  `:=`(
    hours_since_last = as.numeric(
      departed_date - shift(departed_date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(departed_date, type = "lead") - departed_date,
      units = "hours"
    )
  ),
  by = unique_identifier
]

removals_df <-
  removals_df |>
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

removals_df |>
  if_has("unique_identifier", \(d) col_vals_not_null(d, unique_identifier,
    actions = action_levels(warn_at = 0.50, stop_at = 0.75))) |>
  if_has("departed_date", \(d) col_vals_not_null(d, departed_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("departed_date", \(d) col_vals_between(d, departed_date,
    as.Date("2000-01-01"), Sys.Date(), na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("birth_year", \(d) col_vals_between(d, birth_year,
    1900L, as.integer(format(Sys.Date(), "%Y")), na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("gender", \(d) col_vals_in_set(d, gender,
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001))) |>
  if_has(c("duplicate_likely", "unique_identifier"), \(d) col_vals_not_null(d, duplicate_likely,
    preconditions = \(x) dplyr::filter(x, !is.na(unique_identifier)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  invisible()

save_historical_outputs(removals_df, "removals-historical")
