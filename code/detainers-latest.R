# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read ----

col_types <- c(
  "date", # Apprehension Date
  "text", # Apprehension Method
  "text", # Biometric Match Yes No
  "text", # Birth Country
  "numeric", # Birth Year
  "text", # Case Category
  "text", # Case Status
  "text", # Citizenship Country
  "date", # Departed Date
  "text", # Departure Country
  "text", # Deportation Ordered Yes No
  "text", # Detainer Lift Reason
  "text", # Detainer Prepared Threat Level
  "date", # Detainer Prepared Date
  "text", # Detainer Prepared Criminality
  "text", # Detainer Type
  "text", # Detention Facility
  "text", # Detention Facility Code
  "date", # Entry Date
  "text", # Entry Status
  "text", # AOR
  "text", # City
  "text", # State
  "text", # Felon
  "date", # Final Order Date
  "text", # Final Order Yes No
  "text", # TOD Final Program
  "text", # Gender
  "text", # MSC Charge
  "text", # MSC Charge Code
  "text", # MSC Charge Date
  "text", # MSC Conviction Date
  "numeric", # Sentence Days
  "numeric", # Sentence Months
  "numeric", # Sentence Years
  "text", # Order to Show Cause Served Yes No
  "text", # Port Of Departure
  "text", # Processing Disposition
  "text", # Resume Custody Yes No
  "text", # Statements Made Yes No
  "text", # TOA Case Category
  "text", # TOA Case Category Code
  "text", # Alien File Number
  "text" # Anonymized Unique Identifier
)


detainers_df <-
  list.files(
    here::here("inputs"),
    pattern = "^[^~].*DPR",
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

# warnings about date parsing, all in MSC charge and conviction dates, cannot be resolved unambiguously

# ---- Check: read ----
detainers_df |>
  col_exists(
    c(
      `Detainer Prepared Date`,
      `Anonymized Unique Identifier`,
      `Gender`,
      `Case Category`,
      `Detention Facility Code`
    )
  ) |>
  col_vals_not_null(
    `Detainer Prepared Date`,
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
    duplicate_likely = if_else(!is.na(anonymized_unique_identifier), n() > 1, NA),
    .by = c("detainer_prepared_date", "anonymized_unique_identifier")
  ) |>
  rename(
    order_show_cause_served_yes_no = order_to_show_cause_served_yes_no
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Check: clean + duplicates ----
detainers_df |>
  col_exists(
    c(
      detainer_prepared_date,
      anonymized_unique_identifier,
      gender,
      case_status,
      detention_facility_code,
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
  # -- Primary key / identifier checks --
  col_vals_not_null(
    anonymized_unique_identifier,
    actions = action_levels(warn_at = 0.15, stop_at = 0.20)
  ) |>
  col_vals_not_null(
    detainer_prepared_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    detention_facility_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Date range checks --
  col_vals_between(
    detainer_prepared_date,
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
    detainer_prepared_criminality,
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
      "[2V] Voluntary Departure Granted by IJ",
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
      "[8V] Voluntary Departure Granted by IJ",
      "[9] VR Under Safeguards",
      "[H] Historical Category For Migration Only",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    deportation_ordered_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Sentence fields should be non-negative --
  col_vals_gte(
    sentence_days,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    sentence_months,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    sentence_years,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Logical consistency: departed_date implies departure_country --
  col_vals_expr(
    expr(is.na(departed_date) | !is.na(departure_country)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Logical consistency: detainer_prepared_date should be <= departed_date --
  col_vals_expr(
    expr(
      is.na(detainer_prepared_date) |
        is.na(departed_date) |
        detainer_prepared_date <= departed_date
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
  # -- duplicate_likely should not be null for rows with anonymized_unique_identifier --
  col_vals_not_null(
    duplicate_likely,
    preconditions = \(x) dplyr::filter(x, !is.na(anonymized_unique_identifier)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()


# ---- Rename to match prior releases ----
detainers_df <-
  detainers_df |>
  rename(
    detainer_prep_threat_level = detainer_prepared_threat_level,
    most_serious_conviction_charge = msc_charge,
    arrest_time_case_category = toa_case_category,
    msc_sentence_days = sentence_days,
    msc_sentence_months = sentence_months,
    msc_sentence_years = sentence_years,
    unique_identifier = anonymized_unique_identifier
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

# END.
