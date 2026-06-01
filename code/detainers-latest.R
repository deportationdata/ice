# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read ----

col_types_all <- c(
  "date", # Detainer Prepare Date
  "text", # Facility State
  "text", # Facility AOR
  "text", # Port of Departure
  "text", # Departure Country
  "date", # Departed Date
  "text", # Case Status
  "text", # Detainer Criminality
  "text", # Detainer Facility
  "text", # Detainer Facility Code
  "text", # Facility City
  "text", # Detainer Threat Level
  "text", # Gender
  "text", # Citizenship Country
  "text", # Birth Country
  "text", # Birth Date
  "numeric", # Birth Year
  "text", # Entry Status
  "text", # MSC Charge
  "numeric", # MSC Sentence Days
  "numeric", # MSC Sentence Months
  "numeric", # MSC Sentence Years
  "text", # MSC Charge Code
  "date", # MSC Charge Date
  "date", # MSC Conviction Date
  "text", # Aggravated Felon Yes No
  "text", # Processing Disposition
  "text", # Case Category
  "text", # TOA Case Category
  "text", # TOA Current Program
  "text", # Apprehension Method
  "text", # Final Order Yes No
  "date", # Final Order Date
  "date", # Apprehension Date
  "date", # Entry Date
  "text", # Prior Felony Yes No
  "text", # Multiple Prior MISD Yes No
  "text", # Violent Misdemeanor Yes No
  "text", # Illegal Entry Yes No
  "text", # Illegal Reentry Yes No
  "text", # Immigration Fraud Yes No
  "text", # Significant Risk Yes No
  "text", # Other Removal Reason Yes No
  "text", # Other Removal Reason
  "text", # Criminal Street Gang Yes No
  "text", # Aggravated Felony Yes No
  "text", # Deportation Ordered Yes No
  "text", # Order to Show Cause Served Yes No
  "date", # Order to Show Cause Served Date
  "text", # Biometric Match Yes No
  "text", # Statements Made Yes No
  "text", # Unlawful Attempt Yes No
  "text", # Unlawful Entry Yes No
  "text", # Visa Yes No
  "text", # Federal Register Notice Yes No
  "text", # Resume Custody Yes No
  "text", # Detainer Lift Reason
  "text", # Detainer Lift Reason Code
  "text", # Active Investigation Yes No
  "date", # Arrest Warrant Served Date
  "text", # Detainer Type
  "text", # Notify Release Request Yes No
  "text", # TOD Current Duty Site
  "text", # EID DTA ID
  "text" # Anonymized Identifier
)

# FY23 and FY24 files lack the Birth Date column
col_types_fy2324 <- col_types_all[-16]

detainers_df <-
  list.files(
    Sys.getenv("ICE_RAW_DATA_DIR"),
    pattern = "^[^~].*Detainers",
    full.names = TRUE
  ) |>
  set_names(basename) |>
  map_dfr(
    function(f) {
      # FY23/FY24 files lack Birth Date column
      n_cols <- ncol(readxl::read_excel(f, n_max = 0, skip = 6))
      ct <- if (n_cols == length(col_types_all)) {
        col_types_all
      } else {
        col_types_fy2324
      }
      readxl::excel_sheets(f) |>
        set_names() |>
        map_dfr(
          function(s) {
            readxl::read_excel(
              path = f,
              sheet = s,
              col_types = ct,
              skip = 6
            )
          },
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  )
# warnings about date parsing, all in MSC charge and conviction dates, cannot be resolved unambiguously

# ---- Check: read ----
detainers_df |>
  col_exists(
    c(
      `Detainer Prepare Date`,
      `Anonymized Identifier`,
      `Gender`,
      `Case Status`,
      `Detainer Facility Code`
    )
  ) |>
  col_vals_not_null(
    `Detainer Prepare Date`,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()


detainers_df <-
  detainers_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add row number from original file
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = "sheet_original"
  ) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  # replace redacted values with NA
  mutate(across(where(is.character), ~ na_if(.x, "b(6), b(7)c"))) |>
  mutate(across(where(is.character), ~ na_if(.x, "b(6), b(7)C"))) |>
  mutate(
    birth_year = as.integer(birth_year)
  ) |>
  mutate(
    # TODO: wrong
    duplicate_likely = if_else(!is.na(anonymized_identifier), n() > 1, NA),
    .by = c("detainer_prepare_date", "anonymized_identifier")
  ) |>
  rename(
    order_show_cause_served_yes_no = order_to_show_cause_served_yes_no
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Check: clean + duplicates ----
detainers_df |>
  col_exists(
    c(
      detainer_prepare_date,
      anonymized_identifier,
      gender,
      case_status,
      detainer_facility_code,
      file_original,
      sheet_original,
      row_original,
      duplicate_likely,
      order_show_cause_served_yes_no
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

detainers_df |>
  col_vals_expr(
    expr = expr(!if_any(where(is.character), is_redacted)),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  # -- Primary key / identifier checks --
  col_vals_not_null(
    anonymized_identifier,
    actions = action_levels(warn_at = 0.15, stop_at = 0.20)
  ) |>
  col_vals_not_null(
    detainer_prepare_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    detainer_facility_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Date range checks --
  col_vals_between(
    detainer_prepare_date,
    as.Date("2022-09-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    apprehension_date,
    as.Date("1980-01-01"),
    Sys.Date() + 1,
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
    departed_date,
    as.Date("2022-09-01"),
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
    detainer_criminality,
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
    aggravated_felony_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Yes/No flag columns should only contain YES, NO, or NA --
  col_vals_in_set(
    prior_felony_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    deportation_ordered_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Sentence fields should be non-negative --
  col_vals_gte(
    msc_sentence_days,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    msc_sentence_months,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    msc_sentence_years,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Logical consistency: departed_date implies departure_country --
  col_vals_expr(
    expr(is.na(departed_date) | !is.na(departure_country)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Logical consistency: detainer_prepare_date should be <= departed_date --
  col_vals_expr(
    expr(
      is.na(detainer_prepare_date) |
        is.na(departed_date) |
        detainer_prepare_date <= departed_date
    ),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Logical consistency: MSC conviction_date <= charge_date not necessarily,
  #    but charge_date should be reasonable if present --
  col_vals_between(
    msc_charge_date,
    as.Date("1950-01-01"),
    Sys.Date(),
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
detainers_df <-
  detainers_df |>
  rename(
    detainer_prepared_criminality = detainer_criminality,
    detention_facility = detainer_facility,
    detention_facility_code = detainer_facility_code,
    detainer_prep_threat_level = detainer_threat_level,
    most_serious_conviction_charge = msc_charge,
    felon = aggravated_felon_yes_no,
    arrest_time_case_category = toa_case_category,
    arrest_time_current_program = toa_current_program,
    federal_interest_yes_no = federal_register_notice_yes_no,
    unique_identifier = anonymized_identifier
  )


# ---- Save Outputs ----

arrow::write_parquet(
  detainers_df,
  "data/detainers-latest.parquet",
  compression = "zstd"
)
writexl::write_xlsx(detainers_df, "data/detainers-latest.xlsx")
haven::write_dta(detainers_df, "data/detainers-latest.dta")
haven::write_sav(detainers_df, "data/detainers-latest.sav")
