# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.box.com/shared/static/c6tfgtq90er5df898vaq912okchn9o66.xlsx"
f <- tempfile(fileext = ".xlsx")
download.file(url, f, mode = "wb")

col_types <- c(
  "date",    # Stay Book In Date Time
  "date",    # Book In Date Time
  "text",    # Detention Facility
  "text",    # Detention Facility Code
  "date",    # Detention Book Out Date Time
  "date",    # Stay Book Out Date Time
  "text",    # Detention Release Reason
  "date",    # Stay Book Out Date
  "text",    # Stay Release Reason
  "text",    # Religion
  "text",    # Gender
  "text",    # Marital Status
  "text",    # Birth Date (redacted)
  "numeric", # Birth Year
  "text",    # Ethnicity
  "text",    # Entry Status
  "text",    # Felon
  "date",    # Bond Posted Date
  "numeric", # Bond Posted Amount
  "text",    # Case Status
  "text",    # Case Category
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "text",    # Case Threat Level
  "text",    # Book In Criminality
  "text",    # Final Charge
  "date",    # Departed Date
  "text",    # Departure Country
  "numeric", # Initial Bond Set Amount
  "text",    # Citizenship Country
  "text",    # Final Program
  "text",    # Most Serious Conviction (MSC) Charge Code
  "text",    # MSC Charge
  "text",    # Alien File Number (redacted)
  "text",    # EID Case ID (redacted)
  "text",    # EID Subject ID (redacted)
  "text"     # Unique Identifier
)

detentions_df <- 
  readxl::excel_sheets(path = f) |> 
  set_names() |> 
  map_dfr(~readxl::read_excel(path = f, sheet = .x, col_types = col_types, skip = 6), .id = "sheet")

detentions_df <- 
  detentions_df |> 
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  rename(most_serious_conviction_code = most_serious_conviction_msc_charge_code) |>
  # add file name
  mutate(file = "2025-ICLI-00019_2024-ICFO-39357_ICE Detentions_LESA-STU_FINAL Redacted.xlsx") |>
  # add row number from original file
  mutate(row = as.integer(row_number() + 6 + 1)) |> 
  # add identifier for each ICE stay, encompassing multiple detentions or stints
  mutate(stay_ID = str_c(unique_identifier, "_", stay_book_in_date_time)) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |> 
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |> 
  mutate(
    birth_year = as.integer(birth_year)
  ) |> 
  relocate(file, sheet, row, .after = last_col())

# ---- Save Outputs ----
arrow::write_feather(detentions_df, "data/detentions-latest.feather")
haven::write_dta(detentions_df, "data/detentions-latest.dta")
haven::write_sav(detentions_df, "data/detentions-latest.sav")

detentions_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~str_c("Detentions (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/detentions-latest.xlsx")


# # create stay-level data frame

# stay_df <- 
#   detentions_df |> 
#   group_by(stay_ID, stay_book_in_date_time, unique_identifier) |> 
#   summarise(
#     across(
#       c(
#         stay_book_out_date_time, stay_book_out_date,
#         detention_release_reason, stay_release_reason, 
#         departed_date, departure_country
#       ),
#       ~first(na.omit(.x))
#     ),
#     .groups = "drop"
#   )
