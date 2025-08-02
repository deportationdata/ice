# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("clean/functions/check_dttm_and_convert_to_date.R")
source("clean/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_1922082018423"
f <- tempfile(fileext = ".xlsx")
download.file(url, f, mode = "wb")

col_types <- c(
  "date",    # Apprehension Date
  "text",    # Apprehension State
  "logical", # Apprehension County
  "text",    # Apprehension AOR
  "text",    # Final Program
  "text",    # Final Program Group
  "text",    # Apprehension Method
  "text",    # Apprehension Criminality
  "text",    # Case Status
  "text",    # Case Category
  "date",    # Departed Date
  "text",    # Departure Country
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "text",    # Birth Date
  "numeric", # Birth Year
  "text",    # Citizenship Country
  "text",    # Gender
  "text",    # Apprehension Site Landmark
  "text",    # Alien File Number
  "text",    # EID Case ID
  "text",    # EID Subject ID
  "text"     # Unique Identifier
)

arrests_df <- readxl::read_excel(path = f, sheet = 1, col_types = col_types, skip = 6)

arrests_df <- 
  arrests_df |> 
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add file name
  mutate(file = "2025-ICLI-00019_2024-ICFO-39357_ERO Admin Arrests_raw.xlsx") |>
  # add sheets indicator
  mutate(sheet = "Admin Arrests") |> 
  # add row number from original file
  mutate(row = as.integer(row_number() + 6 + 1)) |> 
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |> 
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |> 
  mutate(
    # create apprehension date without time
    apprehension_date_time = apprehension_date,
    apprehension_date = as.Date(apprehension_date_time),
    # convert birth year to integer
    birth_year = as.integer(birth_year)
  )

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other

library(data.table)
setDT(arrests_df)
setorder(arrests_df, apprehension_date_time)

arrests_df[, `:=`(
  hours_since_last = as.numeric(apprehension_date_time - shift(apprehension_date_time, type = "lag"), units = "hours"),
  hours_until_next = as.numeric(shift(apprehension_date_time, type = "lead") - apprehension_date_time, units = "hours")
), by = unique_identifier]

arrests_df <- 
  arrests_df |> 
  as_tibble() |> 
  mutate(
    within_24hrs_prior = !is.na(hours_since_last) & hours_since_last <= 24,
    within_24hrs_next = !is.na(hours_until_next) & hours_until_next <= 24,
    duplicate_possible = case_when(!is.na(unique_identifier) ~ within_24hrs_prior | within_24hrs_next),
  ) |> 
  select(-within_24hrs_prior, -within_24hrs_next, -hours_since_last, -hours_until_next) |> 
  relocate(file, sheet, row, .after = last_col())

# ---- Save Outputs ----

arrow::write_feather(arrests_df, "outputs/arrests-latest.feather")
writexl::write_xlsx(arrests_df, "outputs/arrests-latest.xlsx")
haven::write_dta(arrests_df, "outputs/arrests-latest.dta")
haven::write_sav(arrests_df, "outputs/arrests-latest.sav")
