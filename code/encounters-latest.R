# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.box.com/shared/static/tp02l4356coo3vzjbbksk51olobzis73.xlsx"
f <- tempfile(fileext = ".xlsx")
download.file(url, f, mode = "wb")

col_types <- c(
  "date", # Event Date
  "text", # Responsible AOR
  "text", # Responsible Site
  "text", # Lead Event Type
  "text", # Lead Source
  "text", # Event Type
  "text", # Final Program
  "text", # Final Program Group
  "text", # Encounter Criminality
  "text", # Processing Disposition
  "text", # Case Status
  "text", # Case Category
  "date", # Departed Date
  "text", # Departure Country
  "text", # Final Order Yes No
  "date", # Final Order Date
  "text", # Birth Date
  "numeric", # Birth Year
  "text", # Citizenship Country
  "text", # Gender
  "text", # Event Landmark
  "text", # Alien File Number
  "text", # EID Case ID
  "text", # EID Subject ID
  "text" # Unique Identifier
)

encounters_df <- readxl::read_excel(
  path = f,
  sheet = 1,
  col_types = col_types,
  skip = 6
)

encounters_df <-
  encounters_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add file name
  mutate(
    file = "2025-ICLI-00019_2024-ICFO-39357_ERO Encounters_LESA-STU_FINAL Redacted.xlsx"
  ) |>
  # add sheets indicator
  mutate(sheet = "Encounters") |>
  # add row number from original file
  mutate(row = as.integer(row_number() + 6 + 1)) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  mutate(
    duplicate_likely = if_else(!is.na(unique_identifier), n() > 1, NA),
    .by = c("event_date", "unique_identifier")
  ) |>
  relocate(file, sheet, row, .after = last_col())


# ---- Save Outputs ----

arrow::write_feather(encounters_df, "data/encounters-latest.feather")
writexl::write_xlsx(encounters_df, "data/encounters-latest.xlsx")
haven::write_dta(encounters_df, "data/encounters-latest.dta")
haven::write_sav(encounters_df, "data/encounters-latest.sav")
