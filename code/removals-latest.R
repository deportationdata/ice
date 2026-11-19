# ---- Packages ----
library(tidyverse)
library(tidylog)
library(data.table)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in data ----

col_types <- c(
  "text", # Apprehension State
  "text", # Apprehension City
  "numeric", # Birth Year
  "text", # Case Category
  "text", # TOA Case Category
  "text", # Case Criminality
  "text", # Case Status
  "text", # Case Threat Level
  "text", # Citizenship Country
  "date", # Departed Date
  "text", # Departure Country
  "text", # Docket AOR
  "date", # Entry Date
  "text", # Entry Status
  "text", # Final Charge Code
  "text", # Final Charge Section
  "text", # Final Order Yes No
  "date", # Final Order Date
  "text", # Final Program
  "text", # Final Program Code
  "text", # Gender
  "date", # Apprehension Date
  "text", # Latest Apprehension Program Code
  "text", # Latest Apprehension Program
  "text", # MSC Charge
  "date", # MSC Charge Date
  "text", # MSC Charge Code
  "date", # MSC Conviction Date
  "text", # MSC Criminal Charge Status
  "text", # Port of Departure
  "text", # Prior Deport Yes No
  "text", # Progrocessing Disposition
  "text", # Arresting Agency
  "text", # Alien File Number
  "text" # Anonymized Unique Identifier
)

removals_df <-
  list.files(
    here::here('inputs/'),
    pattern = "^[^~].*REM",
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
# there are date warnings for MSC charge and conviction dates, but they are not fixable

removals_df <-
  removals_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |>
  # add row number from original file
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = c("file_original", "sheet_original")
  ) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  mutate(
    # convert birth year to integer
    birth_year = as.integer(birth_year)
  )

removals_df <-
  removals_df |>
  mutate(
    msc_code = as.character(msc_charge_code),

    # keep only pure 4-digit numeric NCIC-style codes for the UCR logic
    msc4 = if_else(str_detect(msc_code, "^[0-9]{4}$"), msc_code, NA_character_),

    # Homicide (09xx) EXCLUDING negligent manslaughter (0909, 0910)
    ucr_violent = (str_detect(msc4, "^09") & !msc4 %in% c("0909", "0910")) |

      # Rape / Sexual Assault (11xx) EXCLUDING statutory rape - no force (1116)
      (str_detect(msc4, "^11") & msc4 != "1116") |

      # Robbery (12xx)
      str_detect(msc4, "^12") |

      # Aggravated assault ONLY: 1301–1312 plus 1314–1315
      msc4 %in%
        c(
          sprintf("13%02d", 1:12),
          "1314",
          "1315"
        ),
    conviction = case_when(
      ucr_violent ~ "Violent crime",
      !is.na(msc_charge_code) ~ "Nonviolent crime",
      TRUE ~ "None"
    )
  ) |>
  select(-msc_code, -msc4, -ucr_violent)

# ---- Construct Duplicate Episode Indicators ----
setDT(removals_df)

removals_df[,
  `:=`(
    anonymized_identifier_nona = fifelse(
      is.na(anonymized_unique_identifier),
      paste0("noid_", .I),
      anonymized_unique_identifier
    )
  )
]

setorder(
  removals_df,
  anonymized_identifier_nona,
  departed_date,
  departure_country,
  sheet_original,
  row_original,
  na.last = TRUE
)

removals_df[,
  `:=`(
    episode_count = .GRP,
    episode_first = seq_len(.N) == 1L,
    episode_last = seq_len(.N) == .N,
    duplicate_likely = fifelse(
      is.na(anonymized_unique_identifier),
      as.logical(NA),
      .N > 1L
    )
  ),
  by = .(anonymized_identifier_nona, departed_date)
]

# Prefer last record; appear to have better info, but should check
removals_df <-
  removals_df |>
  as_tibble() |>
  mutate(duplicate_drop_row = duplicate_likely & !episode_last)

# ---- Rename and organize to match other datasets ----

removals_df <- removals_df |>
  rename(
    unique_identifier = anonymized_unique_identifier,
    processing_disposition = progrocessing_disposition, # typo in raw data
    final_program = final_program_code, # `final_program` and `final_program_code` swapped in raw data
    final_program_code = final_program
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col()) |>
  arrange(file_original, sheet_original, row_original)

# ---- Save Outputs ----
arrow::write_parquet(
  removals_df,
  "data/removals-latest.parquet",
  compression = "zstd"
)
# writexl::write_xlsx(removals_df, "data/removals-latest.xlsx")
# haven::write_dta(removals_df, "data/removals-latest.dta")
# haven::write_sav(removals_df, "data/removals-latest.sav")
