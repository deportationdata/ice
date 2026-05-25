# Source-file selection
# | df  | folder               | window                   | role    | reason                                                            |
# |-----|----------------------|--------------------------|---------|-------------------------------------------------------------------|
# | df2 | 2023_ICFO_42034      | 2011-10-01 .. 2022-09-30 | use     | only source for pre-2022-10-01; analytical cols redacted          |
# | df5 | March 2026 Release   | 2022-10-01 .. 2025-10-16 | use     | richer cols (state, criminality, gender, etc.); more complete in  |
# |     |                      |                          |         | late-FY23 overlap where df2 starts undercounting (~Aug 2023)      |
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
library(pointblank)
library(haven)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/if_has.R")
source("code/functions/save_historical_outputs.R")
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

col_type_overrides_2023_ICFO_42034_arrests <- c(
  Apprehension_Date = "date",
  Apprehension_Method = "text",
  Arrest_Created_By = "text",
  Arrest_Create_By = "text",
  Arrested_Created_By = "text",
  Case_ID = "text",
  Subject_ID = "text",
  Alien_File_Number = "text",
  Anonymized_Identifier = "text"
)

df2 <- list.files(
  path = "inputs/arrests/2023_ICFO_42034",
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
          \(sh) {
            process_sheet(
              file_path = fp,
              sheet = sh,
              anchor_idx = 2,
              col_type_overrides = col_type_overrides_2023_ICFO_42034_arrests
            )
          },
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  mutate(across(
    where(~ inherits(.x, "POSIXt")),
    check_dttm_and_convert_to_date
  )) |>
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

col_type_overrides_march2026_arrests <- c(
  Apprehension_Date = "date",
  Apprehension_Type = "text",
  State = "text",
  County = "text",
  TOA_Current_Duty_AOR = "text",
  Apprehension_Final_Program = "text",
  Arresting_Agency = "text",
  Apprehension_Method = "text",
  Apprehension_Criminality = "text",
  Case_Status = "text",
  Case_Category = "text",
  Departure_Country = "text",
  Final_Order_Yes_No = "text",
  Birth_Date = "text",
  Birth_Year = "numeric",
  Citizenship_Country = "text",
  Gender = "text",
  Departed_Date = "date",
  Final_Order_Date = "date",
  Apprehension_Site_Landmark = "text",
  Operation = "text",
  TOA_Current_Duty_Site = "text",
  Case_Criminality = "text",
  Case_Threat_Level = "text",
  Anonymized_Identifier = "text"
)

df5 <- list.files(
  path = "inputs/arrests/March 2026 Release",
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
          \(sh) {
            process_sheet(
              file_path = fp,
              sheet = sh,
              anchor_idx = 2,
              guess_max = 10000,
              col_type_overrides = col_type_overrides_march2026_arrests
            )
          },
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  ) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(
    where(~ inherits(.x, "POSIXt")),
    check_dttm_and_convert_to_date
  ))

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

df2_trimmed <-
  df2 |>
  filter(Apprehension_Date < as.Date("2022-10-01")) |>
  rename(
    Unique_Identifier = Anonymized_Identifier,
    Apprehension_Date_Time = Apprehension_Date
  ) |>
  mutate(
    Apprehension_Date_Time = as.POSIXct(Apprehension_Date_Time, tz = "UTC")
  ) |>
  janitor::clean_names(allow_dupes = FALSE)

df5_trimmed <-
  df5 |>
  filter(as.Date(apprehension_date) >= as.Date("2022-10-01")) |>
  rename(apprehension_date_time = apprehension_date) |>
  mutate(
    apprehension_date_time = as.POSIXct(apprehension_date_time, tz = "UTC")
  )

rm(df2, df5)
gc()

source("code/functions/safe_bind_rows.R")

arrests_df <-
  df2_trimmed |>
  safe_bind_rows(df5_trimmed) |>
  redact_to_na() |>
  mutate(across(any_of("birth_year"), as.integer)) |>
  rename(
    apprehension_state = any_of("state"),
    apprehension_aor = any_of("toa_current_duty_aor"),
    final_program = any_of("apprehension_final_program")
  )

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
    within_24hrs_next = !is.na(hours_until_next) & hours_until_next <= 24,

    duplicate_likely = case_when(
      !is.na(unique_identifier) ~ within_24hrs_prior | within_24hrs_next,
      TRUE ~ FALSE
    ),

    # NEW: which rows to drop (drop the later row in a <=24hr pair)
    drop_row = case_when(
      is.na(unique_identifier) ~ FALSE,
      within_24hrs_prior ~ TRUE, # later-than-previous within 24h => drop
      TRUE ~ FALSE
    )
  ) |>
  select(
    -within_24hrs_prior,
    -within_24hrs_next,
    -hours_since_last,
    -hours_until_next
  )

arrests_df |>
  if_has("unique_identifier", \(d) {
    col_vals_not_null(
      d,
      unique_identifier,
      actions = action_levels(warn_at = 0.30, stop_at = 0.50)
    )
  }) |>
  if_has("apprehension_date", \(d) {
    col_vals_not_null(
      d,
      apprehension_date,
      actions = action_levels(warn_at = 0.001, stop_at = 0.01)
    )
  }) |>
  if_has("apprehension_date", \(d) {
    col_vals_between(
      d,
      apprehension_date,
      as.Date("2011-10-01"),
      Sys.Date(),
      na_pass = TRUE,
      actions = action_levels(warn_at = 0.001, stop_at = 0.01)
    )
  }) |>
  if_has("departed_date", \(d) {
    col_vals_between(
      d,
      departed_date,
      as.Date("2011-10-01"),
      Sys.Date(),
      na_pass = TRUE,
      actions = action_levels(warn_at = 0.001, stop_at = 0.01)
    )
  }) |>
  if_has("final_order_date", \(d) {
    col_vals_between(
      d,
      final_order_date,
      as.Date("1990-01-01"),
      Sys.Date(),
      na_pass = TRUE,
      actions = action_levels(warn_at = 0.001, stop_at = 0.01)
    )
  }) |>
  if_has("birth_year", \(d) {
    col_vals_between(
      d,
      birth_year,
      1900L,
      as.integer(format(Sys.Date(), "%Y")),
      na_pass = TRUE,
      actions = action_levels(warn_at = 0.001, stop_at = 0.01)
    )
  }) |>
  if_has("gender", \(d) {
    col_vals_in_set(
      d,
      gender,
      c("Male", "Female", "Unknown", NA),
      actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
    )
  }) |>
  if_has("apprehension_criminality", \(d) {
    col_vals_in_set(
      d,
      apprehension_criminality,
      c(
        "1 Convicted Criminal",
        "2 Pending Criminal Charges",
        "3 Other Immigration Violator",
        NA
      ),
      actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
    )
  }) |>
  if_has("final_order_yes_no", \(d) {
    col_vals_in_set(
      d,
      final_order_yes_no,
      c("YES", "NO", NA),
      actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
    )
  }) |>
  if_has("case_status", \(d) {
    col_vals_in_set(
      d,
      case_status,
      c(
        "ACTIVE",
        "0-Withdrawal Permitted - I-275 Issued",
        "3-Voluntary Departure Confirmed",
        "5-Title 50 Expulsion",
        "6-Deported/Removed - Deportability",
        "7-Died",
        "8-Excluded/Deported/Removed",
        "8-Excluded/Removed - Inadmissibility",
        "9-VR Witnessed",
        "10-USC Prosecution Case Closed",
        "A-Proceedings Terminated",
        "B-Relief Granted",
        "E-Charging Document Canceled by ICE",
        "L-Legalization - Permanent Residence Granted",
        "Z-SAW - Permanent Residence Granted",
        NA
      ),
      actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
    )
  }) |>
  if_has("case_category", \(d) {
    col_vals_in_set(
      d,
      case_category,
      c(
        "[10] Visa Waiver Deportation / Removal",
        "[11] Administrative Deportation / Removal",
        "[12] Judicial Deportation / Removal",
        "[13] Section 250 Removal",
        "[14] Crewmen, Stowaways, S-Visa Holders, 235(c) Cases",
        "[15] Terrorist Court Case (Title 5)",
        "[16] Reinstated Final Order",
        "[17] USC Prosecution Case",
        "[1A] Voluntary Departure - Un-Expired and Un-Extended Departure Period",
        "[1B] Voluntary Departure - Extended Departure Period",
        "[1C] Expired Voluntary Departure Period - Referred to Investigation",
        "[2A] Deportable - Under Adjudication by IJ",
        "[2B] Deportable - Under Adjudication by BIA",
        "[3] Deportable - Administratively Final Order",
        "[5A] Referred for Investigation - No Show for Hearing - No Final Order",
        "[5B] Removable - ICE Fugitive",
        "[5C] Relief Granted - Withholding of Deportation / Removal",
        "[5D] Final Order of Deportation / Removal - Deferred Action Granted",
        "[5E] Relief Granted - Extended Voluntary Departure",
        "[5F] Unable to Obtain Travel Document",
        "[8A] Excludable / Inadmissible - Hearing Not Commenced",
        "[8B] Excludable / Inadmissible - Under Adjudication by IJ",
        "[8C] Excludable / Inadmissible - Administrative Final Order Issued",
        "[8D] Excludable / Inadmissible - Under Adjudication by BIA",
        "[8E] Inadmissible - ICE Fugitive",
        "[8F] Expedited Removal",
        "[8G] Expedited Removal - Credible Fear Referral",
        "[8H] Expedited Removal - Status Claim Referral",
        "[8I] Inadmissible - ICE Fugitive - Expedited Removal",
        "[8K] Expedited Removal Terminated due to Credible Fear Finding / NTA Issued",
        "[9] VR Under Safeguards",
        "[H] Historical Category For Migration Only",
        NA
      ),
      actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
    )
  }) |>
  if_has(c("departed_date", "departure_country"), \(d) {
    col_vals_expr(
      d,
      expr(is.na(departed_date) | !is.na(departure_country)),
      actions = action_levels(warn_at = 0.01, stop_at = 0.05)
    )
  }) |>
  if_has(c("final_order_date", "final_order_yes_no"), \(d) {
    col_vals_expr(
      d,
      expr(is.na(final_order_date) | final_order_yes_no == "YES"),
      na_pass = TRUE,
      actions = action_levels(warn_at = 0.01, stop_at = 0.05)
    )
  }) |>
  if_has(c("duplicate_likely", "unique_identifier"), \(d) {
    col_vals_not_null(
      d,
      duplicate_likely,
      preconditions = \(x) dplyr::filter(x, !is.na(unique_identifier)),
      actions = action_levels(warn_at = 0.001, stop_at = 0.01)
    )
  }) |>
  invisible()

save_historical_outputs(arrests_df, "arrests-historical")
