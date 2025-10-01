# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.box.com/shared/static/hrofkgyefmmvki95f487rpk1wigcefd5.xlsx"
f <- tempfile(fileext = ".xlsx")
download.file(url, f, mode = "wb")

# col_types <- c(
#   "date", # Apprehension Date
#   "text", # Apprehension State
#   "logical", # Apprehension County
#   "text", # Apprehension AOR
#   "text", # Final Program
#   "text", # Final Program Group
#   "text", # Apprehension Method
#   "text", # Apprehension Criminality
#   "text", # Case Status
#   "text", # Case Category
#   "date", # Departed Date
#   "text", # Departure Country
#   "text", # Final Order Yes No
#   "date", # Final Order Date
#   "text", # Birth Date
#   "numeric", # Birth Year
#   "text", # Citizenship Country
#   "text", # Gender
#   "text", # Apprehension Site Landmark
#   "text", # Alien File Number
#   "text", # EID Case ID
#   "text", # EID Subject ID
#   "text" # Unique Identifier
# )

col_types <-
  c(
    "date", # Departed Date
    "text", # Port of Departure
    "text", # Departure Country
    "text", # Docket AOR
    "text", # Apprehension State
    "text", # Apprehension County
    "text", # Case Status
    "text", # Gender
    "text", # Birth Country
    "text", # Citizenship Country
    "text", # Birth Date
    "numeric", # Birth Year
    "date", # Entry Date
    "text", # Entry Status
    "text", # MSC NCIC Charge
    "text", # MSC NCIC Charge Code
    "date", # MSC Charge Date
    "text", # MSC Criminal Charge Status
    "text", # MSC Criminal Charge Status Code
    "date", # MSC Conviction Date
    "text", # Case Threat Level
    "text", # Case Criminality
    "text", # Felon
    "text", # Processing Disposition
    "text", # Case Category
    "text", # Final Program
    "text", # Final Program Code
    "text", # Case Category Time of Arrest
    "text", # Latest Arrest Program Current
    "text", # Latest Arrest Program Current Code
    "text", # Latest Person Apprehension Date
    "text", # Final Order Yes No
    "date", # Final Order Date
    "text", # Final Charge Code
    "text", # Final Charge Section Code
    "text", # Prior Deport Yes No
    "date", # Latest Person Departed Date
    "text", # Alien File Number
    "text", # EID Case ID
    "text", # EID Subject ID
    "text" # Unique Identifier
  )

removals_df <-
  readxl::read_excel(
    path = f,
    sheet = 1,
    col_types = col_types,
    skip = 6
  )
# there are 23 date warnings for MSC charge and conviction dates, but they are not fixable

removals_df <-
  removals_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add file name
  mutate(
    file_original = "2025-ICLI-00019_2024-ICFO-39357_ICE Removals_LESA-STU_FINAL Redacted.xlsx"
  ) |>
  # add sheets indicator
  mutate(sheet_original = "Removals") |>
  # add row number from original file
  mutate(row_original = as.integer(row_number() + 6 + 1)) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  mutate(
    # convert birth year to integer
    birth_year = as.integer(birth_year)
  )

# # ---- Construct Duplicates Indicator ----
# # two (or more) arrests within 24 hours of each other

# library(data.table)
# setDT(removals_df)
# setorder(removals_df, departed_date)

# removals_df[,
#   `:=`(
#     days_since_last = as.numeric(
#       departed_date - shift(departed_date, type = "lag"),
#       units = "days"
#     ),
#     days_until_next = as.numeric(
#       shift(departed_date, type = "lead") - departed_date,
#       units = "days"
#     )
#   ),
#   by = unique_identifier
# ]

# removals_df <-
#   removals_df |>
#   as_tibble() |>
#   mutate(
#     within_24hrs_prior = !is.na(days_since_last) & days_since_last <= 1,
#     within_24hrs_next = !is.na(days_until_next) & days_until_next <= 1,
#     duplicate_likely = case_when(
#       !is.na(unique_identifier) ~ within_24hrs_prior | within_24hrs_next
#     ),
#   ) |>
#   select(
#     -within_24hrs_prior,
#     -within_24hrs_next,
#     -days_since_last,
#     -days_until_next
#   ) |>
#   relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Save Outputs ----

arrow::write_feather(removals_df, "data/removals-latest.feather")
writexl::write_xlsx(removals_df, "data/removals-latest.xlsx")
haven::write_dta(
  removals_df |>
    rename(latest_arrest_program_code = latest_arrest_program_current_code),
  "data/removals-latest.dta"
)
haven::write_sav(removals_df, "data/removals-latest.sav")
