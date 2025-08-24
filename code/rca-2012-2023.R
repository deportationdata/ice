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

url <- "https://ucla.box.com/shared/static/x6jxz982onabumgfx0dwnd39b27ggoqz.zip"
f <- tempfile(fileext = ".zip")
download.file(url, f, mode = "wb")
unzip(f, exdir = temp_dir)
files <- list.files(temp_dir, pattern = "xlsx", full.names = TRUE)

col_types <- c(
  "text",    # RCA_AOR
  "text",    # RCA_DCO
  "text",    # A_NUMBER
  "text",    # SUBJ_ID
  "text",    # LAST_NAME
  "text",    # FIRST_NAME
  "text",    # ALERT_CODE
  "text",    # ACTIVE_INACTIVE
  "text",    # SUBMISSION_DATE
  "text",    # STATUS_CODE
  "text",    # RISK_TO_PUBLIC_SAFETY
  "text",    # RISK_OF_FLIGHT
  "text",    # SPECIAL_VULNERABILITY
  "text",    # RCA_CASE_NUMBER
  "text",    # CASE_CAT_AT_RCA_DECISION
  "text",    # FO_AT_RCA_DECISION
  "text",    # FO_DATE_AT_RCA_DECISION
  "text",    # REMOVAL_LIKELY_AT_RCA_DECISION
  "text",    # MAN_DET_PER_STAT_ALLEG
  "text",    # RCA_DECISION_TYPE
  "text",    # RCA_RECOMMENDATION
  "text",    # RCA_BOND_RECOMMENDATION
  "text",    # OFFICER_ID
  "text",    # OFFICER_AGREE_DISAGREE
  "text",    # SUPERVISOR_ID
  "text",    # SUPERVISOR_AGREE_DISAGREE
  "text",    # RCA_FINAL_DECISION
  "numeric", # FINAL_BOND_AMOUNT
  "date",    # RCA_DECISION_DATE
  "text",    # RCA_SCORING_VER
  "text",    # SPEC_VULN_VER
  "text",    # MAN_DET_VER
  "text",    # DISC_INFR_VER
  "text",    # fiscal_year_code
  "text",    # fiscal_quarter_name
  "text",    # ANONYMIZED_IDENTIFIER
  "text",    # SPECIAL_VULNERABILITY_COMMENTS
  "text",    # REASON_REMOVAL_UNLIKELY_AT_RCA_DECISION
  "text",    # OFFICER_COMMENTS
  "text",    # SUPERVISOR_COMMENTS
  "text",    # SUBJ ID
  "text",    # A Number
  "text",    # LAST NAME
  "text",    # FIRST NAME
  "text",    # RCA_CASE NUMBER
  "text",    # OFFICER ID
  "text",    # SUPERVISOR ID
  "text",    # A NUMBER
  "text",    # SUJB ID
  "text"     # RCA CASE NUMBER
)

rcas_df <- 
  files |> 
  set_names(nm = basename(files)) |> 
  map(function(fl) {
    readxl::excel_sheets(fl) |> 
      set_names() |> 
      map(
        ~readxl::read_excel(path = fl, sheet = .x, skip = 5) |> colnames(), 
      )
  })

rcas_df <- 
  files |> 
  set_names(nm = basename(files)) |> 
  map_dfr(function(fl) {
    readxl::excel_sheets(fl) |> 
      set_names() |> 
      map_dfr(
        ~readxl::read_excel(path = fl, sheet = .x, skip = 5), 
        .id = "sheet"
      )
  }, .id = "file")

rcas_df2 <- 
  rcas_df |> 
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

arrow::write_feather(rcas_df, "outputs/ice-rcas-2012-2023.feather")
# haven::write_dta(rcas_df, "outputs/ice-rcas-2012-2023.dta")
# haven::write_sav(rcas_df, "outputs/ice-rcas-2012-2023.sav")

rcas_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~str_c("Sheet ", seq_along(.x), "")) |>
  writexl::write_xlsx("outputs/ice-rcas-2012-2023.xlsx")
