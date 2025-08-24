# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# read in to temporary file
temp_dir <- tempdir(check = TRUE)
unlink(temp_dir, recursive = TRUE)
dir.create(temp_dir)

url <- "https://ucla.box.com/shared/static/hanv0cqbvxole910mo63z9uy40cwwi9t.zip"
f <- tempfile(fileext = ".zip")
download.file(url, f, mode = "wb")
unzip(f, exdir = temp_dir)
files <- list.files(temp_dir, pattern = "xlsx", full.names = TRUE)

col_types <- c(
  "date",    # Departure Date
  "text",    # Port of Departure
  "text",    # Departure Country
  "text",    # Case Status
  "text",    # Case Category
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "text",    # Case ID
  "text",    # Gender
  "text",    # Birth Country
  "text",    # Citizenship Country
  "text",    # Birth Date
  "numeric", # Birth Year
  "text",    # Alien File Number
  "text",    # Entry Status
  "date",    # Entry Date
  "text",    # MSC Charge
  "date",    # MSC Charge Date
  "text",    # MSC Charge Code
  "date",    # MSC Conviction Date
  "text",    # MSC Criminal Charge Status
  "text",    # Case Threat Level
  "text",    # Processing Disposition Code
  "text",    # Processing Disposition
  "text",    # Current Program
  "date",    # Apprehension Date
  "text",    # Charge Section Code
  "text",    # Charge Code
  "text"     # Anonymized Identifer
)

removals_df <- 
  files |> 
  set_names(nm = basename(files)) |> 
  map_dfr(function(fl) {
    readxl::excel_sheets(fl) |> 
      set_names() |> 
      map_dfr(
        ~readxl::read_excel(path = fl, sheet = .x, col_types = col_types, skip = 5), 
        .id = "sheet"
      )
  }, .id = "file")

removals_df <- 
  removals_df |> 
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add row number from original file
  mutate(row = as.integer(row_number() + 6), .by = c("file", "sheet"), .after = sheet) |> 
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |> 
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |> 
  relocate(file, sheet, row, .after = last_col())

# ---- Save Outputs ----

arrow::write_feather(removals_df, "data/ice-removals-2012-2023.feather")
haven::write_dta(removals_df, "data/ice-removals-2012-2023.dta")
haven::write_sav(removals_df, "data/ice-removals-2012-2023.sav")

removals_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~str_c("Sheet ", seq_along(.x), "")) |>
  writexl::write_xlsx("data/ice-removals-2012-2023.xlsx")
