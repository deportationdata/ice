# ---- Packages ----
library(tidyverse)
library(tidylog)
library(data.table)

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

# ---- Flag for whether removal associated with detention stay ----
pre_join_rows <- nrow(removals_df)

removals_with_anon_id_deduped <- removals_df |>
  filter(!is.na(anonymized_unique_identifier), duplicate_drop_row == FALSE) |>
  mutate(removal_ID = row_number())

removals_no_anon_id_and_dupes <- removals_df |>
  filter(is.na(anonymized_unique_identifier) | duplicate_drop_row == TRUE)

detentions_df_deduped <- detentions_df |>
  filter(!is.na(unique_identifier)) |>
  distinct(.keep_all = TRUE)

# ---- Match stays to removals ----
removal_detention_pairs <-
  detentions_df_deduped |>
  select(stay_ID, unique_identifier, stay_book_out_date_time) |>
  inner_join(
    removals_with_anon_id_deduped |>
      select(
        anonymized_unique_identifier,
        departed_date,
        departure_country,
        removal_ID
      ),
    by = c("unique_identifier" = "anonymized_unique_identifier"),
    relationship = "many-to-many"
  ) |>
  mutate(
    time_diff = as.numeric(difftime(
      stay_book_out_date_time,
      departed_date,
      units = "hours"
    ))
  ) |>
  # keep removals 1 days before to 10 days after the stay_book_out_date_time
  filter(time_diff <= 24 * 1, time_diff >= 24 * -10) |>
  # then keep the closest removal per book-out and the closest book-out per removal
  slice_min(
    order_by = abs(time_diff),
    n = 1,
    with_ties = FALSE,
    by = stay_ID
  ) |>
  slice_min(
    order_by = abs(time_diff),
    n = 1,
    with_ties = FALSE,
    by = removal_ID
  ) |>
  select(stay_ID, removal_ID)

# ---- Merge stays onto removals ----

detention_ids <- unique(detentions_df$unique_identifier)

removals_with_detentions <- removals_with_anon_id_deduped |>
  left_join(
    removal_detention_pairs,
    by = "removal_ID",
    relationship = "one-to-one"
  ) |>
  mutate(
    has_detention_stay = !is.na(stay_ID),
    id_in_detentions = anonymized_unique_identifier %in% detention_ids
  )

# Better to drop any of these cols that we don't want to output, but keeping for now
# # Dupes with unique IDs lack detention stay flags
removals_no_anon_id_and_dupes$removal_ID <- NA
removals_no_anon_id_and_dupes$stay_ID <- NA
removals_no_anon_id_and_dupes$has_detention_stay <- NA
removals_no_anon_id_and_dupes$id_in_detentions <- NA

# Fill in detention stays for duplicate groups
removals_df <- rbind(removals_with_detentions, removals_no_anon_id_and_dupes) |>
  group_by(anonymized_unique_identifier, episode_count) |>
  fill(
    has_detention_stay,
    stay_ID,
    removal_ID,
    id_in_detentions,
    .direction = "downup"
  )

stopifnot(nrow(removals_df) == pre_join_rows)

# ---- Rename and organize to match other datasets ----

removals_df <- removals_df |>
  rename(
    unique_identifier = anonymized_unique_identifier,
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col()) |>
  arrange(file_original, sheet_original, row_original)

# ---- Save Outputs ----

# ---- Save Outputs ----
arrow::write_parquet(
  removals_df,
  "data/removals-latest.parquet",
  compression = "zstd"
)
# writexl::write_xlsx(removals_df, "data/removals-latest.xlsx")
# haven::write_dta(removals_df, "data/removals-latest.dta")
# haven::write_sav(removals_df, "data/removals-latest.sav")
