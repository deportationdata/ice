# Source-file selection
# | df  | folder  | window                   | role | reason                             |
# |-----|---------|--------------------------|------|------------------------------------|
# | df1 | March 2026 Release | (whole)       | use  | recent release                     |
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
library(pointblank)
library(haven)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/if_has.R")
source("code/functions/coalesce_rename.R")
source("code/functions/save_historical_outputs.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
col_types_march2026_encounters <- c(
  "date",    # Event Date
  "text",    # Event Type
  "text",    # Event Landmark
  "text",    # Operation
  "text",    # Encounter Threat Level
  "text",    # Responsible AOR
  "text",    # Responsible Site
  "text",    # Lead Event Type
  "text",    # Lead Source
  "text",    # Final Program
  "text",    # Arresting Agency
  "text",    # Encounter Criminality
  "text",    # Processing Disposition
  "text",    # Case Status
  "text",    # Case Category
  "date",    # Departed Date
  "text",    # Departure Country
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "numeric", # Birth Year
  "text",    # Citizenship County
  "text",    # Gender
  "text"     # Anonymized Identifier
)

df1 <- list.files(
    path = "inputs/encounters/March 2026 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) read_excel(fp, sheet = sh, col_types = col_types_march2026_encounters, skip = 6),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  rename_with(\(nms) nms |> str_replace_all("\\s+", "_") |> make_clean_names(case = "none")) |>
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(across(ends_with("_Date"), as.Date))

df2 <- list.files(
    path = "inputs/encounters/uwchr",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) {
      excel_sheets(fp) |>
        set_names() |>
        map_dfr(
          \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000),
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))

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
  safe_bind_rows(
    df2 |> rename(
      Responsible_AOR = Area_of_Responsibility,
      Event_Landmark  = Landmark
    )
  ) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  coalesce_rename("unique_identifier", "anonymized_identifier") |>
  mutate(event_date = as.Date(event_date)) |>
  redact_to_na() |>
  mutate(across(any_of("birth_year"), as.integer))

# Free per-source dfs now that they're merged
rm(df1, df2)
gc()

# FLAG Duplicates
setDT(encounters_df)
setorder(encounters_df, unique_identifier, event_date)

encounters_df[,
  `:=`(
    hours_since_last = as.numeric(
      event_date - shift(event_date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(event_date, type = "lead") - event_date,
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

encounters_df |>
  if_has("event_date", \(d) col_vals_not_null(d, event_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("event_date", \(d) col_vals_between(d, event_date,
    as.Date("2007-01-01"), Sys.Date(), na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("birth_year", \(d) col_vals_between(d, birth_year,
    1900L, as.integer(format(Sys.Date(), "%Y")), na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("gender", \(d) col_vals_in_set(d, gender,
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001))) |>
  if_has("final_order_yes_no", \(d) col_vals_in_set(d, final_order_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001))) |>
  if_has(c("duplicate_likely", "unique_identifier"), \(d) col_vals_not_null(d, duplicate_likely,
    preconditions = \(x) dplyr::filter(x, !is.na(unique_identifier)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  invisible()

save_historical_outputs(encounters_df, "encounters-historical")
