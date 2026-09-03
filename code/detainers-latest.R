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
  "date", # MSC Charge Date
  "date", # MSC Conviction Date
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

# one date parsing warning: FY2026 Apprehension Date (Excel row 47273) holds text "06/14/0002 03:10 PM", coerced to NA

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
    .by = c("file_original", "sheet_original")
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
  mutate(across(where(is.character), ~ na_if(.x, "NA"))) |>
  mutate(
    birth_year = as.integer(birth_year)
  ) |>
  mutate(
    apprehension_method_simple = case_when(
      apprehension_method %in%
        c(
          "Non-Custodial Arrest",
          "Located",
          "Probation and Parole",
          "Worksite Enforcement"
        ) ~ "At-Large Arrest",
      apprehension_method %in%
        c(
          "Custodial Arrest",
          "CAP Local Incarceration",
          "CAP Federal Incarceration",
          "CAP State Incarceration",
          "Criminal Alien Program"
        ) ~ "Custodial Arrest",
      apprehension_method == "287(g) Program" ~ "287(g) Program",
      is.na(apprehension_method) ~ NA_character_,
      TRUE ~ "Other"
    )
  ) |>
  mutate(
    request_type = case_when(
      str_detect(detainer_type, "I247A") ~ "Detainer request",
      str_detect(detainer_type, "I247D") ~ "Detainer request",
      str_detect(detainer_type, "I247G") ~ "Request for advance notification of release",
      str_detect(detainer_type, "I247N") ~ "Request for advance notification of release",
      str_detect(detainer_type, "I247X") ~ "Other",
      str_detect(detainer_type, "I247 ") ~ "Detainer request"
    )
  )

# Detainers table has MSC charge and code variables so we don't need to join in
detainers_df <-
  detainers_df |>
  mutate(
    msc_code = as.character(msc_charge_code),

    # keep only pure 4-digit numeric NCIC-style codes for the UCR logic
    msc4 = if_else(str_detect(msc_code, "^[0-9]{4}$"), msc_code, NA_character_),

    # Homicide (09xx) EXCLUDING negligent manslaughter (0909, 0910)
    ucr_violent = (str_detect(msc4, "^09") & !msc4 %in% c("0909", "0910")) |

      # Rape / Sexual Assault (11xx) EXCLUDING statutory rape - no force (1116)
      (str_detect(msc4, "^11") & msc4 != "1116") |

      # Robbery (12xx)
      str_detect(msc4, "^12") |

      # Aggravated assault ONLY: 1301–1312 plus 1314–1315
      msc4 %in%
        c(
          sprintf("13%02d", 1:12),
          "1314",
          "1315"
        ),
    conviction = case_when(
      ucr_violent ~ "Violent crime",
      !is.na(msc_charge_code) ~ "Nonviolent crime",
      TRUE ~ "None"
    )
  ) |>
  select(-msc_code, -msc4, -ucr_violent)

# ---- Construct Duplicate Episode Indicators ----

library(data.table)
setDT(detainers_df)

detainers_df[,
           `:=`(
             anonymized_identifier_nona = fifelse(
               is.na(anonymized_unique_identifier),
               paste0("noid_", .I),
               anonymized_unique_identifier
             )
           )
]

setorder(
  detainers_df,
  anonymized_identifier_nona,
  detainer_prepared_date,
  file_original,
  sheet_original,
  row_original,
  na.last = TRUE
)

detainers_df[,
           duplicate_episode_identifier := {
             gap <- as.numeric(
               detainer_prepared_date - shift(detainer_prepared_date, type = "lag"),
               units = "hours"
             )
             cumsum(is.na(gap) | gap > 24)
           },
           by = .(anonymized_identifier_nona)
]

detainers_df[,
           `:=`(
             duplicate_episode_first = seq_len(.N) == 1L,
             duplicate_likely = fifelse(
               is.na(anonymized_unique_identifier),
               as.logical(NA),
               .N > 1L
             )
           ),
           by = .(anonymized_identifier_nona,
                  duplicate_episode_identifier,
                  detainer_type,
                  detention_facility)
]

detainers_df[, c("anonymized_identifier_nona") := NULL]

detainers_df <-
  detainers_df |>
  as_tibble() |>
  mutate(duplicate_drop_row = duplicate_likely & !duplicate_episode_first)

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
      order_to_show_cause_served_yes_no
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
    conviction,
    c(
      "Violent crime",
      "Nonviolent crime",
      "None"
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
      "D-IJ/BIA Dismissed",
      "E-Charging Document Canceled by ICE",
      "L-Legalization - Permanent Residence Granted",
      "P-Policy closure",
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

# ---- Rename to match March 2026 release ----
detainers_df <-
  detainers_df |>
  rename(
    detainer_prepare_date = detainer_prepared_date,
    facility_aor = aor,
    facility_city = city,
    facility_state = state,
    detainer_prep_threat_level = detainer_prepared_threat_level,
    most_serious_conviction_charge = msc_charge,
    arrest_time_case_category = toa_case_category,
    arrest_time_case_category_code = toa_case_category_code,
    msc_sentence_days = sentence_days,
    msc_sentence_months = sentence_months,
    msc_sentence_years = sentence_years,
    unique_identifier = anonymized_unique_identifier,
    order_show_cause_served_yes_no = order_to_show_cause_served_yes_no
  ) |>
  relocate(unique_identifier, .before = duplicate_likely) |>
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Save Outputs ----

arrow::write_parquet(
  detainers_df,
  "data/detainers-latest.parquet",
  compression = "zstd"
)
# writexl::write_xlsx(detainers_df, "data/detainers-latest.xlsx")
# haven::write_dta(detainers_df, "data/detainers-latest.dta")
# haven::write_sav(detainers_df, "data/detainers-latest.sav")

# END.
