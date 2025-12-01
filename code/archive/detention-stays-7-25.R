# ---- Packages ----
library(tidyverse)
library(tidylog)
library(data.table)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.box.com/shared/static/c6tfgtq90er5df898vaq912okchn9o66.xlsx"
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
    file = "2025-ICLI-00019_2024-ICFO-39357_ICE Detentions_LESA-STU_FINAL Redacted.xlsx"
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
  filter(!is.na(unique_identifier)) |>
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

# ---- Resolve multiple stints within stay based on bond amount ----

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

detentions_df <-
  detentions_df[,
    .SD[
      if (all(is.na(initial_bond_set_amount))) {
        .N
      } else {
        tail(
          which(
            initial_bond_set_amount ==
              min(initial_bond_set_amount, na.rm = TRUE)
          ),
          1
        )
      }
    ],
    by = stint_ID
  ]

setorder(
  detentions_df,
  unique_identifier,
  book_in_date_time,
  detention_book_out_date_time,
  detention_facility_code
)

# ---- Create stay-level dataset ----

detentions_df[, first_stay := .I == .I[1], by = stay_ID]
detentions_df[, last_stay := .I == .I[.N], by = stay_ID]
detentions_df[,
  longest_stay := {
    detention_book_out_date_time_imputed <- fifelse(
      is.na(detention_book_out_date_time),
      as.POSIXct("2025-07-28 23:59:59", tz = "UTC"),
      detention_book_out_date_time
    )
    diff <- detention_book_out_date_time_imputed - book_in_date_time
    longest <- which(diff == max(diff, na.rm = TRUE))
    longest <- longest[length(longest)] # In case of ties, take the last one
    .I == .I[longest]
  },
  by = stay_ID
]

detention_facility_df_first <-
  detentions_df |>
  filter(first_stay == TRUE) |>
  select(
    stay_ID,
    detention_facility_first = detention_facility,
    detention_facility_code_first = detention_facility_code,
    book_in_date_time_first = book_in_date_time,
    book_out_date_time_first = detention_book_out_date_time
  )

detention_facility_df_longest <-
  detentions_df |>
  filter(longest_stay == TRUE) |>
  select(
    stay_ID,
    detention_facility_longest = detention_facility,
    detention_facility_code_longest = detention_facility_code,
    book_in_date_time_longest = book_in_date_time,
    book_out_date_time_longest = detention_book_out_date_time
  )

detention_facility_df_last <-
  detentions_df |>
  filter(last_stay == TRUE) |>
  select(
    stay_ID,
    detention_facility_last = detention_facility,
    detention_facility_code_last = detention_facility_code,
    book_in_date_time_last = book_in_date_time,
    book_out_date_time_last = detention_book_out_date_time
  )

detention_stay_level_vars_df <-
  detentions_df[,
    c(
      list(
        n_stints = .N,
        detention_facility_codes_all = str_c(
          detention_facility_code,
          collapse = "; "
        )
      ),
      lapply(
        .SD,
        function(x) tail(x, 1)
      )
    ),
    by = stay_ID,
    .SDcols = setdiff(
      names(detentions_df),
      c(
        "stay_ID",
        "detention_facility",
        "detention_facility_code",
        "book_in_date_time",
        "detention_book_out_date_time"
      )
    )
  ]

detention_individual_level_vars_df <-
  detention_stay_level_vars_df[, .(n_stays = .N), by = unique_identifier]

detention_stays_df <-
  detentions_df |>
  distinct(stay_ID) |>
  left_join(detention_stay_level_vars_df, by = "stay_ID") |>
  left_join(detention_individual_level_vars_df, by = "unique_identifier") |>
  left_join(detention_facility_df_first, by = "stay_ID") |>
  left_join(detention_facility_df_longest, by = "stay_ID") |>
  left_join(detention_facility_df_last, by = "stay_ID") |>
  select(-first_stay, -last_stay, -longest_stay, -file, -sheet, -row) |>
  as_tibble()

# ---- Save Outputs ----

arrow::write_feather(detention_stays_df, "data/detention-stays-latest.feather")
haven::write_dta(detention_stays_df, "data/detention-stays-latest.dta")
haven::write_sav(detention_stays_df, "data/detention-stays-latest.sav")
writexl::write_xlsx(detention_stays_df, "data/detention-stays-latest.xlsx")
