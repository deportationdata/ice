# Source-file selection
# | df  | folder  | window                   | role | reason                             |
# |-----|---------|--------------------------|------|------------------------------------|
# | df1 | 082025  | (whole)                  | use  | recent release                     |
# | df2 | uwchr   | (whole)                  | use  | historical coverage                |

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
source("code/functions/process_folder_data.R")
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
df1 <- get_folder_df0(
  folder_dir = "inputs/encounters/082025",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "082025")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df2 <- list.files(
    path = "inputs/encounters/uwchr",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  map_dfr(\(fp) {
    excel_sheets(fp) |>
      map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
  }) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(source_file = "uwchr")

# # --- Weekly counts for source-coverage check ---
# df1_weekly_counts <- get_weekly_counts(df1, "Event_Date")
# df2_weekly_counts <- get_weekly_counts(df2, "Event_Date")

# all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts)|>
#   arrange(week_start)

# ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
#   geom_line(alpha = 0.5)+
#   theme_minimal()

# boundaries <- all_weekly_counts |>
#   group_by(source_file) |>
#   summarise(start = min(week_start), end = max(week_start))

source("code/functions/safe_bind_rows.R")

encounters_df <- df1 |>
  rename(Event_Date_Time = Event_Date) |>
  mutate(Event_Date_Time = as.POSIXct(Event_Date_Time, tz = "UTC")) |>
  safe_bind_rows(
    df2 |>
      rename(
        Event_Date_Time = Event_Date,
        Responsible_AOR = Area_of_Responsibility,
        Event_Landmark  = Landmark
      ) |>
      mutate(Event_Date_Time = as.POSIXct(Event_Date_Time, tz = "UTC"))
  ) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(event_date = as.Date(event_date_time))

# Free per-source dfs now that they're merged
rm(list = setdiff(ls(), "encounters_df"))
gc()

# FLAG Duplicates
setDT(encounters_df)
setorder(encounters_df, unique_identifier, event_date_time)

encounters_df[,
  `:=`(
    hours_since_last = as.numeric(
      event_date_time - shift(event_date_time, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(event_date_time, type = "lead") - event_date_time,
      units = "hours"
    )
  ),
  by = unique_identifier
]

encounters_df <-
  encounters_df |>
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

write_parquet(encounters_df, "data/encounters-historical.parquet", compression = "zstd", compression_level = 19)
