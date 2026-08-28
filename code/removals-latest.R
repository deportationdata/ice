# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in data ----

detentions_df <- arrow::read_parquet("data/detention-stays-latest.parquet")

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
  "text", # Final Order Date
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
    .by = "sheet_original"
  ) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  mutate(
    # convert birth year to integer
    birth_year = as.integer(birth_year)
  ) |>
  mutate(
    duplicate_likely = if_else(!is.na(anonymized_unique_identifier), n() > 1, NA),
    .by = c("departed_date", "anonymized_unique_identifier")
  )

# Removals table has MSC charge and code variables so we don't need to join in

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

# Join with detentions
# Need to deal with duplicates generated in join

removals_with_anon_id <- removals_df |>
  filter(!is.na(anonymized_unique_identifier))

removals_no_anon_id <- removals_df |>
  filter(is.na(anonymized_unique_identifier))

detention_subset <- detentions_df |>
  mutate(has_detention = TRUE,
         stay_book_out_date_time_minus_1 = stay_book_out_date_time - days(1)) |>
  select(unique_identifier,
         departure_country,
         stay_book_out_date_time_minus_1,
         has_detention)

has_detention <- removals_with_anon_id |>
  mutate(departed_date_plus_1 = departed_date + days(1)) |>
  left_join(detention_subset, by =
              join_by(anonymized_unique_identifier == unique_identifier,
                      departure_country == departure_country,
                      departed_date_plus_1 >= stay_book_out_date_time_minus_1
                                           )) |>
  mutate(id_in_detentions = anonymized_unique_identifier %in% unique(detention_subset$unique_identifier)) |>
  group_by(file_original, sheet_original, row_original) %>%
  mutate(is_duplicate = n() > 1,
         duplicate_first = is_duplicate == TRUE & n() == 1 # This doesn't work
         ) %>% 
  ungroup()

removals_df <- removals_df |>
  rename(
    unique_identifier = anonymized_unique_identifier,
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Save Outputs ----

arrow::write_feather(removals_df, "data/removals-latest.feather")
removals_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~ str_c("Removals (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/removals-latest.xlsx")
# haven::write_dta(
#   removals_df |>
#     rename(latest_arrest_program_code = latest_arrest_program_current_code), # What is this??
#   "data/removals-latest.dta"
# )
# haven::write_sav(removals_df, "data/removals-latest.sav")
