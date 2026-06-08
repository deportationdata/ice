# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in files ----

col_types <- c(
  "date", # Event Date
  "text", # Event Type
  "text", # Event Landmark
  "text", # Operation
  "text", # Encounter Threat Level
  "text", # Responsible AOR
  "text", # Responsible Site
  "text", # Lead Event Type
  "text", # Lead Source
  "text", # Final Program
  "text", # Arresting Agency
  "text", # Encounter Criminality
  "text", # Processing Disposition
  "text", # Case Status
  "text", # Case Category
  "date", # Departed Date
  "text", # Departure Country
  "text", # Final Order Yes No
  "date", # Final Order Date
  "numeric", # Birth Year
  "text", # Citizenship County
  "text", # Gender
  "text" # Anonymized Identifier
)

encounters_df <-
  list.files(
    Sys.getenv("ICE_RAW_DATA_DIR"),
    pattern = "^[^~].*Encounters",
    full.names = TRUE
  ) |>
  set_names(basename) |>
  map_dfr(
    function(f) {
      readxl::excel_sheets(f) |>
        set_names() |>
        map_dfr(
          function(s) {
            readxl::read_excel(
              path = f,
              sheet = s,
              col_types = col_types,
              skip = 6
            )
          },
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  )

encounters_df <-
  encounters_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add row number from original file
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = "sheet_original"
  ) |>
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
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Save Outputs ----

arrow::write_feather(encounters_df, "data/encounters-latest.feather")
# writexl::write_xlsx(encounters_df, "data/encounters-latest.xlsx")
# haven::write_dta(encounters_df, "data/encounters-latest.dta")
# haven::write_sav(encounters_df, "data/encounters-latest.sav")
