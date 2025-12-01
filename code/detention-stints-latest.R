# ---- Packages ----
library(tidyverse)
library(tidylog)
library(data.table)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.box.com/shared/static/l55xu3n0r7xnovjrc5ak1fksan2dsj70.xlsx"
f <- tempfile(fileext = ".xlsx")
download.file(url, f, mode = "wb")

col_types <- c(
  "date", # Stay Book In Date Time
  "date", # Book In Date Time
  "text", # Detention Facility
  "text", # Detention Facility Code
  "date", # Detention Book Out Date Time
  "date", # Stay Book Out Date Time
  "text", # Detention Release Reason
  "date", # Stay Book Out Date
  "text", # Stay Release Reason
  "text", # Religion
  "text", # Gender
  "text", # Marital Status
  "text", # Birth Date (redacted)
  "numeric", # Birth Year
  "text", # Ethnicity
  "text", # Entry Status
  "text", # Felon
  "date", # Bond Posted Date
  "numeric", # Bond Posted Amount
  "text", # Case Status
  "text", # Case Category
  "text", # Final Order Yes No
  "date", # Final Order Date
  "text", # Case Threat Level
  "text", # Book In Criminality
  "text", # Final Charge
  "date", # Departed Date
  "text", # Departure Country
  "numeric", # Initial Bond Set Amount
  "text", # Citizenship Country
  "text", # Final Program
  "text", # Most Serious Conviction (MSC) Charge Code
  "text", # MSC Charge
  "text", # Alien File Number (redacted)
  "text", # EID Case ID (redacted)
  "text", # EID Subject ID (redacted)
  "text" # Unique Identifier
)

# ---- Read in data ----

detentions_df <-
  readxl::excel_sheets(path = f) |>
  set_names() |>
  map_dfr(
    ~ readxl::read_excel(path = f, sheet = .x, col_types = col_types, skip = 6),
    .id = "sheet"
  )

# ---- Initial cleaning of stint-level data ----

detentions_df <-
  detentions_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  rename(
    most_serious_conviction_code = most_serious_conviction_msc_charge_code
  ) |>
  # add file name
  mutate(
    file = "ICE Detentions_LESA-STU_FINAL Release.xlsx"
  ) |>
  # add row number from original file
  mutate(row = as.integer(row_number() + 6 + 1)) |>
  # add identifier for each ICE stay, encompassing multiple detentions or stints
  mutate(stay_ID = str_c(unique_identifier, "_", stay_book_in_date_time)) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  mutate(
    birth_year = as.integer(birth_year)
  ) |>
  relocate(file, sheet, row, .after = last_col()) |>
  # filter(!is.na(unique_identifier)) |>
  mutate(
    stint_ID = str_c(
      unique_identifier,
      "_",
      book_in_date_time,
      "_",
      detention_facility_code
    )
  )

detentions_df <- as.data.table(detentions_df)

# ---- Create flag for multiple stints within stay based on bond amount ----

setorder(
  detentions_df,
  unique_identifier,
  book_in_date_time,
  detention_facility_code,
  detention_release_reason,
  stay_book_out_date,
  stay_release_reason,
  religion,
  gender,
  marital_status,
  birth_year,
  ethnicity,
  entry_status,
  felon,
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
  final_program,
  most_serious_conviction_code,
  msc_charge
)

detentions_df[,
  likely_duplicate := {
    if (all(is.na(unique_identifier))) {
      rep(NA, .N)
    } else if (all(is.na(initial_bond_set_amount))) {
      seq_len(.N) != .N # tag all but the last row as TRUE
    } else {
      !seq_len(.N) %in%
        tail(
          which(
            initial_bond_set_amount ==
              min(initial_bond_set_amount, na.rm = TRUE)
          ),
          1
        )
    }
  },
  by = stint_ID
]

# ---- Save Outputs ----

arrow::write_feather(detentions_df, "data/detention-stints-latest.feather")
haven::write_dta(detentions_df, "data/detention-stints-latest.dta")
haven::write_sav(detentions_df, "data/detention-stints-latest.sav")

detentions_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~ str_c("Detention stints (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/detention-stints-latest.xlsx")
