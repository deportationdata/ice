# Source-file selection
# | df  | folder  | window                   | role | reason                             |
# |-----|---------|--------------------------|------|------------------------------------|
# | df1 | March 2026 Release | >= 2022-10-01 | use  | richer cols (case status, criminality, etc.)|
# | df2 | uwchr   | < 2022-10-01             | use  | only source for pre-2022-10-01     |

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
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/if_has.R")
source("code/functions/coalesce_rename.R")
source("code/functions/save_historical_outputs.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
col_type_overrides_march2026_encounters <- c(
  Event_Date             = "date",
  Event_Type             = "text",
  Event_Landmark         = "text",
  Operation              = "text",
  Encounter_Threat_Level = "text",
  Responsible_AOR        = "text",
  Responsible_Site       = "text",
  Lead_Event_Type        = "text",
  Lead_Source            = "text",
  Final_Program          = "text",
  Arresting_Agency       = "text",
  Encounter_Criminality  = "text",
  Processing_Disposition = "text",
  Case_Status            = "text",
  Case_Category          = "text",
  Departed_Date          = "date",
  Departure_Country      = "text",
  Final_Order_Yes_No     = "text",
  Final_Order_Date       = "date",
  Birth_Year             = "numeric",
  Citizenship_County     = "text",
  Gender                 = "text",
  Anonymized_Identifier  = "text"
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
      \(sh) process_sheet(
        file_path = fp,
        sheet = sh,
        anchor_idx = 2,
        guess_max = 10000,
        col_type_overrides = col_type_overrides_march2026_encounters
      ),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  filter(as.Date(Event_Date) >= as.Date("2022-10-01"))

col_type_overrides_uwchr_encounters <- c(
  Area_of_Responsibility = "text",
  Event_Date             = "date",
  Landmark               = "text",
  Operation              = "text",
  Processing_Disposition = "text",
  Citizenship_Country    = "text",
  Gender                 = "text",
  Encounter_Threat_Level = "text",
  Alien_File_Number      = "text"
)

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
          \(sh) process_sheet(
            file_path = fp,
            sheet = sh,
            anchor_idx = 2,
            col_type_overrides = col_type_overrides_uwchr_encounters
          ),
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  filter(as.Date(Event_Date) < as.Date("2022-10-01")) |>
  rename(
    Responsible_AOR = Area_of_Responsibility,
    Event_Landmark  = Landmark
  )

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
  safe_bind_rows(df2) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  coalesce_rename("unique_identifier", "anonymized_identifier") |>
  mutate(event_date = as.Date(event_date)) |>
  make_redactions_na() |>
  mutate(across(any_of("birth_year"), as.integer))

rm(df1, df2)
gc()

# ---- Construct Duplicate Episode Indicators ----
setDT(encounters_df)

encounters_df[,
  unique_identifier_nona := fifelse(
    is.na(unique_identifier),
    paste0("noid_", .I),
    unique_identifier
  )
]

setorder(
  encounters_df,
  unique_identifier_nona,
  event_date,
  file_original,
  sheet_original,
  row_original
)

encounters_df[,
  duplicate_episode_id := {
    gap <- as.numeric(
      event_date - shift(event_date, type = "lag"),
      units = "hours"
    )
    cumsum(is.na(gap) | gap > 24)
  },
  by = unique_identifier_nona
]

encounters_df[, unique_identifier_nona := NULL]

encounters_df <- as_tibble(encounters_df)

encounters_df |>
  col_vals_expr(
    expr = expr(!if_any(where(is.character), is_redacted)),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
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
  invisible()

save_historical_outputs(encounters_df, "encounters-historical")
