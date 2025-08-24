# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# read in to temporary file
url_11_16 <- "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_1836555467571"
f_11_16 <- tempfile(fileext = ".xlsx")
download.file(url_11_16, f_11_16, mode = "wb")

url_17_23 <- "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_1836542152676"
f_17_23 <- tempfile(fileext = ".xlsx")
download.file(url_17_23, f_17_23, mode = "wb")

col_types <- c(
  "date",    # Apprehension Date
  "text",    # Apprehension Method
  "text",    # Arrest Created By
  "text",    # Case ID
  "text",    # Subject ID
  "text",    # Alien File Number
  "text"     # Anonymized Identifier
)

col_nms <- c(
  "apprehension_date", 
  "apprehension_method", 
  "arrest_created_by", 
  "case_id", 
  "subject_id", 
  "alien_file_number", 
  "unique_identifier"
)

arrests_11_16 <- 
  readxl::excel_sheets(f_11_16) |> 
  set_names() |> 
  map_dfr(
    ~readxl::read_excel(path = f_11_16, sheet = .x, col_names = col_nms, col_types = col_types, skip = 6), .id = "sheet"
  ) 

arrests_17_23  <- 
  readxl::excel_sheets(f_17_23) |>
  set_names() |> 
  map_dfr(
    ~readxl::read_excel(path = f_17_23, sheet = .x, col_names = col_nms, col_types = col_types, skip = 6), .id = "sheet"
  )

arrests_df <- 
  bind_rows(
    "2023-ICFO_42034_Admin Arrests_FY16-12_LESA-STU_FINAL-Redacted_raw.xlsx" = arrests_11_16,
    "2023-ICFO_42034_Admin Arrests_FY23-17_LESA-STU_FINAL-Redacted_raw.xlsx" = arrests_17_23,
    .id = "file"
  )

arrests_df <- 
  arrests_df |> 
  # add row number from original file
  mutate(row = as.integer(row_number() + 6), .by = c("file", "sheet"), .after = sheet) |> 
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |> 
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |> 
  mutate(dupe_flag_day = if_else(!is.na(unique_identifier), n() > 1, NA), .by = c("apprehension_date", "unique_identifier"))

# ---- Save Outputs ----

arrow::write_feather(arrests_df, "data/ice-arrests-2012-2023.feather")
# writexl::write_xlsx(arrests_df, "data/ice-arrests-2012-2023.xlsx")
haven::write_dta(arrests_df, "data/ice-arrests-2012-2023.dta")
haven::write_sav(arrests_df, "data/ice-arrests-2012-2023.sav")
