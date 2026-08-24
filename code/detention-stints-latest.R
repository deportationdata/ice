# ---- Packages ----
library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

# ---- Input data ---
facilities_df <- arrow::read_parquet(
  "https://media.githubusercontent.com/media/deportationdata/ice-detention-facilities/main/data/facilities-latest.parquet"
) |>
  select(-field_office) |>
  distinct(detention_facility_code, .keep_all = TRUE)

# `msc_charge` : `most_serious_conviction_code` correspondence table from previous release
msc_charge_codes_tbl <- read_csv(here::here("data/msc-charge-codes.csv"))

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

col_types <- c(
  "numeric", # Birth Year
  "numeric", # Bond Posted Amount
  "date", # Bond Posted Date
  "text", # Book In Criminality
  "date", # Detention Book In Date Time
  "text", # Case Category
  "text", # Case Status
  "text", # Case Threat Level
  "text", # Citizenship Country
  "date", # Departed Date
  "text", # Departure Country
  "date", # Detention Book Out Date Time
  "text", # Detention Facility
  "text", # Detention Facility Code
  "text", # Detention Release Reason
  "date", # Entry Date
  "text", # Entry Status
  "text", # Ethnicity
  "text", # Felon
  "text", # Final Charge
  "date", # Final Order Date
  "text", # Final Order Yes No
  "text", # Apprehension Final Program
  "text", # Gender
  "text", # Initial Bond Set Amount
  "text", # Marital Status
  "text", # MSC Charge
  "text", # Religion
  "date", # Stay Book In Date Time
  "date", # Stay Book Out Date Time
  "text", # Stay Release Reason
  "text", # Apprehension Final Program Group
  "text", # Alien File Number
  "text" # Anonymized Unique Identifier
)

# ---- Read in data ----

detentions_df <-
  list.files(
    here::here("inputs/"),
    pattern = "^[^~].*DET",
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
detentions_df |>
  col_exists(
    c(
      `Stay Book In Date Time`,
      `Detention Book In Date Time`,
      `Detention Facility Code`,
      `Anonymized Unique Identifier`,
      `Gender`,
      `Case Status`
    )
  ) |>
  col_vals_not_null(
    `Stay Book In Date Time`,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

# ---- Initial cleaning of stint-level data ----
detentions_df <-
  detentions_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add row number from original file
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = "sheet_original"
  ) |>
  # add identifier for each ICE stay, encompassing multiple detentions or stints
  mutate(
    stay_ID = str_c(anonymized_unique_identifier, "_", stay_book_in_date_time)
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
  relocate(file_original, sheet_original, row_original, .after = last_col()) |>
  # filter(!is.na(anonymized_identifier)) |>
  mutate(
    stint_ID = str_c(
      anonymized_unique_identifier,
      "_",
      detention_book_in_date_time,
      "_",
      detention_facility_code
    )
  )

# ---- New processed vars ----

detentions_df <-
  detentions_df |>
  mutate(
    stint_length_days = as.numeric(
      detention_book_out_date_time - detention_book_in_date_time,
      units = "days"
    ),
    stay_length_days = as.numeric(
      stay_book_out_date_time - stay_book_in_date_time,
      units = "days"
    )
  )

detentions_df <-
  detentions_df |>
  left_join(
    msc_charge_codes_tbl,
    by = "msc_charge",
    relationship = "many-to-one"
  )

detentions_df <-
  detentions_df |>
  mutate(
    msc_code = as.character(most_serious_conviction_code),

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
      !is.na(most_serious_conviction_code) ~ "Nonviolent crime",
      TRUE ~ "None"
    )
  ) |>
  select(-msc_code, -msc4, -ucr_violent)

detentions_df <- as.data.table(detentions_df)

# ---- Check: clean ----
detentions_df |>
  col_exists(
    c(
      stay_ID,
      stint_ID,
      anonymized_unique_identifier,
      detention_facility_code,
      detention_book_in_date_time,
      birth_year,
      file_original,
      sheet_original,
      row_original
    )
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    stint_ID,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  invisible()


# ---- Create duplicate flags ----
setorder(
  detentions_df,
  anonymized_unique_identifier,
  detention_book_in_date_time,
  detention_facility_code,
  detention_release_reason,
  stay_book_out_date_time,
  stay_release_reason,
  religion,
  gender,
  marital_status,
  birth_year,
  ethnicity,
  entry_status,
  bond_posted_date,
  bond_posted_amount,
  case_status,
  case_category,
  final_order_yes_no,
  final_order_date,
  case_threat_level,
  book_in_criminality,
  final_charge,
  departed_date,
  departure_country,
  citizenship_country,
  apprehension_final_program,
  msc_charge
)

detentions_df[, row_idx := .I]

stage1 <- copy(detentions_df[!is.na(anonymized_unique_identifier)])
stage1_dedup_cols <- setdiff(
  names(stage1),
  c(
    "file_original",
    "sheet_original",
    "row_original",
    "row_idx",
    "initial_bond_set_amount",
    "bond_posted_amount",
    "bond_posted_date"
  )
)
stage1_unique <- unique(stage1, by = stage1_dedup_cols)
stage1_kept <- stage1_unique$row_idx

stage1_unique[, stint_date := as.Date(detention_book_in_date_time)]
stage2_unique <- stage1_unique[,
  .SD[.N],
  by = .(
    anonymized_unique_identifier,
    detention_facility_code,
    stint_date,
    stay_ID
  )
]
stage2_kept <- stage2_unique$row_idx

detentions_df[,
  `:=`(
    duplicate_likely_sameday = !is.na(anonymized_unique_identifier) &
      row_idx %in% stage1_kept &
      !row_idx %in% stage2_kept,
    duplicate_drop_row = !is.na(anonymized_unique_identifier) &
      !row_idx %in% stage2_kept
  )
]

detentions_df[,
  duplicate_likely := fifelse(
    is.na(anonymized_unique_identifier),
    NA,
    duplicate_likely_sameday
  )
]

detentions_df[, row_idx := NULL]
detentions_df[, stint_date := as.Date(detention_book_in_date_time)]
rm(stage1, stage1_unique, stage2_unique, stage1_kept, stage2_kept)

n_before_facility_join <- nrow(detentions_df)
detentions_df <-
  detentions_df |>
  left_join(
    facilities_df |>
      select(
        detention_facility_code,
        city,
        state,
        county,
        core_based_statistical_area,
        core_based_statistical_area_code,
        core_based_statistical_area_type,
        #  type_ddp, # Or whatever eventual name is
        starts_with("federal_court")
      ),
    by = "detention_facility_code",
    relationship = "many-to-one"
  )
stopifnot(nrow(detentions_df) == n_before_facility_join)

# ---- Check: duplicate flags + facility join ----
detentions_df |>
  col_exists(
    c(
      duplicate_likely_sameday,
      duplicate_likely,
      duplicate_drop_row,
      detention_book_out_date_time,
      city,
      state,
      county
    )
  ) |>
  col_vals_in_set(
    duplicate_likely_sameday,
    c(TRUE, FALSE),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    duplicate_likely,
    c(TRUE, FALSE, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    duplicate_drop_row,
    c(TRUE, FALSE),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    detention_book_out_date_time,
    preconditions = \(x) dplyr::filter(x, !is.na(stay_book_out_date_time)),
    actions = action_levels(warn_at = 0.05, stop_at = 0.10)
  ) |>
  invisible()


# ---- Pointblank Validation ----

detentions_df |>
  # -- Primary key / identifier checks --
  col_vals_not_null(
    anonymized_unique_identifier,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    stint_ID,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    detention_facility_code,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    detention_book_in_date_time,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Date range checks --
  col_vals_between(
    detention_book_in_date_time,
    as.POSIXct("2000-01-01", tz = "UTC"),
    Sys.time(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    detention_book_out_date_time,
    as.POSIXct("2022-09-01", tz = "UTC"),
    Sys.time(),
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
    marital_status,
    c("Divorced", "Married", "Separated", "Single", "Unknown", "Widowed", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    ethnicity,
    c("Hispanic Origin", "Not of Hispanic Origin", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    book_in_criminality,
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
    case_threat_level,
    c("1", "2", "3", "NA", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Bond amounts should be non-negative --
  col_vals_gte(
    initial_bond_set_amount,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    bond_posted_amount,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Logical consistency: book_in <= book_out --
  col_vals_expr(
    expr(
      is.na(detention_book_in_date_time) |
        is.na(detention_book_out_date_time) |
        detention_book_in_date_time <= detention_book_out_date_time
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Logical consistency: stay_book_in <= stay_book_out --
  col_vals_expr(
    expr(
      is.na(stay_book_in_date_time) |
        is.na(stay_book_out_date_time) |
        stay_book_in_date_time <= stay_book_out_date_time
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Logical consistency: book_in >= stay_book_in (stint starts after stay starts) --
  col_vals_expr(
    expr(
      is.na(detention_book_in_date_time) |
        is.na(stay_book_in_date_time) |
        detention_book_in_date_time >= stay_book_in_date_time
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
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
  # -- duplicate_likely should not be null when anonymized_identifier is present --
  col_vals_expr(
    expr(is.na(anonymized_unique_identifier) | !is.na(duplicate_likely)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    duplicate_drop_row,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  rows_distinct(
    vars(
      anonymized_unique_identifier,
      detention_facility_code,
      stint_date,
      stay_ID
    ),
    preconditions = \(x) {
      dplyr::filter(
        x,
        duplicate_drop_row == FALSE,
        !is.na(anonymized_unique_identifier)
      )
    },
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()


# ---- Rename to match previous releases ----
detentions_df <-
  detentions_df |>
  rename(
    final_program = apprehension_final_program,
    unique_identifier = anonymized_unique_identifier,
    book_in_date_time = detention_book_in_date_time,
    book_out_date_time = detention_book_out_date_time
  )

# ---- Sort by book-in date ----
detentions_df <- detentions_df |>
  arrange(book_in_date_time)

detentions_df$stint_date <- NULL

# ---- Save Outputs ----

arrow::write_parquet(
  detentions_df,
  "data/detention-stints-latest.parquet",
  compression = "zstd"
)
detentions_df |>
  rename_with(
    ~ make.unique(
      abbreviate(.x, minlength = 32, strict = FALSE),
      sep = "_"
    )
  ) |>
  haven::write_dta("data/detention-stints-latest.dta")
haven::write_sav(detentions_df, "data/detention-stints-latest.sav")

detentions_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~ str_c("Detention stints (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/detention-stints-latest.xlsx")

# END.
