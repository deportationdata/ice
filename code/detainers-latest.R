# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in to temporary file ----
url <- "https://ucla.box.com/shared/static/k699orsf5wm8i9ehr1x4foownlj5zuyn.xlsx"
f <- tempfile(fileext = ".xlsx")
download.file(url, f, mode = "wb")

col_types <- c(
  "date",    # Detainer Prepare Date
  "text",    # Facility State
  "text",    # Facility AOR
  "text",    # Port of Departure
  "text",    # Departure Country
  "date",    # Departed Date
  "text",    # Case Status
  "text",    # Detainer Prepared Criminality
  "text",    # Detention Facility
  "text",    # Detention Facility Code
  "text",    # Facility City
  "text",    # Detainer Prep Threat Level
  "text",    # Gender
  "text",    # Citizenship Country
  "text",    # Birth Country
  "text",    # Birth Date
  "numeric", # Birth Year
  "text",    # Entry Status
  "text",    # Most Serious Conviction (MSC) Charge
  "numeric", # MSC Sentence Days
  "numeric", # MSC Sentence Months
  "numeric", # MSC Sentence Years
  "text",    # MSC Charge Code
  "date",    # MSC Charge Date
  "date",    # MSC Conviction Date
  "text",    # Felon
  "text",    # Processing Disposition
  "text",    # Case Category
  "text",    # Final Program
  "text",    # Time of Apprehension Case Category
  "text",    # Time of Apprehension Current Program
  "text",    # Apprehension Method
  "text",    # Case Final Order Yes No
  "date",    # Final Order Date
  "date",    # Apprehension Date
  "date",    # Entry Date
  "text",    # Prior Felony Yes No
  "text",    # Multiple Prior MISD Yes No
  "text",    # Violent Misdemeanor Yes No
  "text",    # Illegal Entry Yes No
  "text",    # Illegal Reentry Yes No
  "text",    # Immigration Fraud Yes No
  "text",    # Significant Risk Yes No
  "text",    # Other Removal Reason Yes No
  "text",    # Other Removal Reason
  "text",    # Criminal Street Gang Yes No
  "text",    # Aggravated Felony Yes No
  "text",    # Deportation Ordered Yes No
  "text",    # Order to Show Cause Served Yes No
  "date",    # Order to Show Cause Served Date
  "text",    # Biometric Match Yes No
  "text",    # Statements Made Yes No
  "text",    # Unlawful Attempt Yes No
  "text",    # Unlawful Entry Yes No
  "text",    # Visa Yes No
  "text",    # Final Order Yes No
  "text",    # Federal Interest Yes No
  "text",    # Resume Custody Yes No
  "text",    # Detainer Lift Reason
  "text",    # Detainer Type
  "text",    # Alien File Number
  "text",    # EID Case ID
  "text",    # EID Subject ID
  "text",    # EID DTA ID
  "text"     # Unique Identifier
)

detainers_df <- readxl::read_excel(path = f, sheet = 1, col_types = col_types, skip = 6)

detainers_df <- 
  detainers_df |> 
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add file name
  mutate(file = "2025-ICLI-00019_2024-ICFO-39357_ERO Detainers_LESA-STU_FINAL Redacted.xlsx") |>
  # add sheets indicator
  mutate(sheet = "Detainers") |> 
  # add row number from original file
  mutate(row = as.integer(row_number() + 6 + 1)) |> 
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |> 
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |> 
  relocate(file, sheet, row, .after = last_col()) 

# ---- Save Outputs ----

arrow::write_feather(detainers_df, "data/detainers-latest.feather")
writexl::write_xlsx(detainers_df, "data/detainers-latest.xlsx")
haven::write_dta(detainers_df, "data/detainers-latest.dta")
haven::write_sav(detainers_df, "data/detainers-latest.sav")
