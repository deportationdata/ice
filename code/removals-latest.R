# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in ----

col_types <- c(
  "date", # Departed Date
  "text", # Port of Departure
  "text", # Departure Country
  "text", # Case AOR
  "text", # State
  "text", # County
  "text", # Case Status
  "text", # Gender
  "text", # Birth Country
  "text", # Citizenship Country
  "text", # Birth Date
  "numeric", # Birth Year
  "date", # Entry Date
  "text", # Entry Status
  "text", # Known Terrorist Yes No
  "text", # Suspected Gang Yes No
  "text", # MSC Charge
  "date", # MSC Charge Date
  "text", # MSC Criminal Charge Status
  "text", # MSC Charge Code
  "date", # MSC Conviction Date
  "text", # Case Criminality
  "text", # Case Threat Level
  "text", # Aggravated Felon Yes No
  "text", # Processing Disposition
  "text", # Case Category
  "text", # Final Program
  "text", # Final Program Code
  "text", # Arresting Agency
  "text", # TOA Case Category
  "text", # Latest Apprehension Final Program
  "text", # Latest Arresting Agency
  "date", # Latest Apprehension Date
  "text", # Final Order Yes No
  "date", # Final Order Date
  "text", # Final Charge Code
  "text", # Final Charge Section
  "text", # Prior Deport Yes No
  "text" # Anonymized Identifier
)

removals_df <-
  list.files(
    Sys.getenv("ICE_RAW_DATA_DIR"),
    pattern = "^[^~].*Removals.*\\.xlsx$",
    full.names = TRUE
  ) |>
  set_names(basename) |>
  map_dfr(
    function(f) {
      readxl::excel_sheets(f) |>
        set_names() |>
        map_dfr(
          function(s) {
            readxl::read_excel(
              path = f,
              sheet = s,
              col_types = col_types,
              skip = 6
            )
          },
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  )

# ---- Check: read ----
removals_df |>
  col_exists(
    c(`Departed Date`, `Anonymized Identifier`, `Gender`, `Case Status`)
  ) |>
  col_vals_not_null(
    `Departed Date`,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()


removals_df <-
  removals_df |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = "sheet_original"
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  mutate(across(where(is.character), ~ na_if(.x, "b(6), b(7)c"))) |>
  mutate(across(where(is.character), ~ na_if(.x, "b(6), b(7)C"))) |>
  mutate(
    departed_date = as.Date(departed_date),
    birth_year = as.integer(birth_year)
  ) |>
  mutate(
    duplicate_likely = if_else(!is.na(anonymized_identifier), n() > 1, NA),
    .by = c("departed_date", "anonymized_identifier")
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Check: clean + duplicates ----
removals_df |>
  col_exists(
    c(
      departed_date,
      anonymized_identifier,
      gender,
      case_status,
      file_original,
      sheet_original,
      row_original,
      duplicate_likely,
      birth_year
    )
  ) |>
  col_vals_not_null(
    row_original,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    duplicate_likely,
    c(TRUE, FALSE, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()


# ---- Pointblank Validation ----

removals_df |>
  # -- Primary key / identifier checks --
  col_vals_not_null(
    anonymized_identifier,
    actions = action_levels(warn_at = 0.02, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    departed_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Date range checks --
  col_vals_between(
    departed_date,
    as.Date("2022-09-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    entry_date,
    as.Date("1900-01-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    final_order_date,
    as.Date("1990-01-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    latest_apprehension_date,
    as.Date("1980-01-01"),
    Sys.Date() + 1,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    msc_charge_date,
    as.Date("1950-01-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Birth year range --
  col_vals_between(
    birth_year,
    1900L,
    as.integer(format(Sys.Date(), "%Y")),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Categorical value checks --
  col_vals_in_set(
    gender,
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    case_criminality,
    c(
      "1 Convicted Criminal",
      "2 Pending Criminal Charges",
      "3 Other Immigration Violator",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    final_order_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    prior_deport_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    known_terrorist_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    suspected_gang_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
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
  ) |>
  col_vals_in_set(
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
  ) |>
  col_vals_in_set(
    case_threat_level,
    c("1", "2", "3", "NA", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Logical consistency: departed_date implies departure_country --
  col_vals_expr(
    expr(is.na(departed_date) | !is.na(departure_country)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Logical consistency: final_order_date only when final_order = YES --
  col_vals_expr(
    expr(is.na(final_order_date) | final_order_yes_no == "YES"),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- duplicate_likely should not be null for rows with anonymized_identifier --
  col_vals_not_null(
    duplicate_likely,
    preconditions = \(x) dplyr::filter(x, !is.na(anonymized_identifier)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()


# ---- Rename to match Oct 2025 release ----
removals_df <-
  removals_df |>
  rename(
    apprehension_state = state,
    apprehension_county = county,
    felon = aggravated_felon_yes_no,
    arrest_time_case_category = toa_case_category,
    most_serious_conviction_charge = msc_charge,
    unique_identifier = anonymized_identifier
  )


# ---- Save Outputs ----

arrow::write_parquet(
  removals_df,
  "data/removals-latest.parquet",
  compression = "zstd"
)
haven::write_dta(
  removals_df |>
    rename(
      latest_apprehension_program = latest_apprehension_final_program
    ),
  "data/removals-latest.dta"
)
haven::write_sav(removals_df, "data/removals-latest.sav")
removals_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~ str_c("Removals (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/removals-latest.xlsx")
