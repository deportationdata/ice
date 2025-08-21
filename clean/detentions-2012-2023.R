# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("clean/functions/check_dttm_and_convert_to_date.R")
source("clean/functions/is_not_blank_or_redacted.R")

temp_dir <- tempdir(check = TRUE)
unlink(temp_dir, recursive = TRUE)
dir.create(temp_dir)

url <- "https://ucla.box.com/shared/static/gjfcedj3ne4d6l6rt5sv0t6yfao18jzp.zip"
f <- tempfile(fileext = ".zip")
download.file(url, f, mode = "wb")
unzip(f, exdir = temp_dir)
files <- list.files(temp_dir, pattern = "xlsx", full.names = TRUE)

col_types <- c(
  "text",    # Detention ID
  "text",    # Case ID
  "text",    # Subject ID
  "date",    # Stay Book In Date
  "date",    # Detention Book In Date
  "text",    # Detention Facility
  "text",    # Detention Facility Code
  "date",    # Detention Book Out Date
  "text",    # Detention Release Reason
  "date",    # Stay Book Out Date
  "text",    # Stay Release Reason
  "text",    # Religion
  "text",    # Marital
  "text",    # Gender
  "text",    # Birth Date
  "text",    # Ethnicity
  "text",    # Alien File Number
  "numeric", # Birth Year
  "text",    # Entry Status
  "date",    # Bond Posted Date
  "numeric", # Bond Posted Amount
  "numeric", # Initial Bond Set Amount
  "text",    # Case Status
  "text",    # Case Category
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "date",    # Departed Date
  "text",    # Departure Country
  "text",    # Case Threat Level
  "text",    # Charge
  "text",    # Charge Code
  "text",    # Charge Section Code
  "text"     # Anonymized Identifier
)

detentions_df <- 
  files |> 
  set_names(nm = basename(files)) |> 
  map_dfr(function(fl) {
    readxl::excel_sheets(fl) |> 
      set_names() |> 
      map_dfr(
        ~readxl::read_excel(path = fl, col_types = col_types, sheet = .x, skip = 5), 
        .id = "sheet"
      )
  }, .id = "file")

detentions_df <- 
  detentions_df |> 
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add row number from original file
  mutate(row = as.integer(row_number() + 5 + 1), .by = c("file", "sheet")) |> 
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |> 
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |> 
  relocate(file, sheet, row, .after = last_col())

arrow::write_feather(detentions_df, "outputs/ice-detentions-2012-2023.feather")
haven::write_dta(detentions_df, "outputs/ice-detentions-2012-2023.dta")
haven::write_sav(detentions_df, "outputs/ice-detentions-2012-2023.sav")

detentions_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~str_c("Sheet ", seq_along(.x), "")) |>
  writexl::write_xlsx("outputs/ice-detentions-2012-2023.xlsx")
