rm(list=ls())

library(arrow)
library(data.table)
library(dplyr)
library(tidyverse)
library(tidylog)

# reference code: detention-stints-latest


# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

df <- read_feather("data/ice-final/detentions-final-v3.feather")

detentions_df <- df |> 
  janitor::clean_names(allow_dupes = FALSE)|>
  rename(
    most_serious_conviction_charge = msc_charge, 
    most_serious_conviction_criminal_charge_category = most_serious_conviction_msc_criminal_charge_category, 
    most_serious_conviction_conviction_date = msc_conviction_date, 
    most_serious_conviction_sentence_days = msc_sentence_days, 
    most_serious_conviction_sentence_months = msc_sentence_months, 
    most_serious_conviction_sentence_years = msc_sentence_years, 
    most_serious_conviction_crime_class = msc_crime_class
  )|>
  select(where(is_not_blank_or_redacted))|>
  mutate(stay_ID = str_c(unique_identifier, "_", stay_book_in_date_time))|>
  mutate(
    stint_ID = str_c(
      unique_identifier,
      "_",
      detention_book_in_date,
      "_",
      detention_facility_code
    )
  )

detentions_df <- as.data.table(detentions_df)
# ---- Create flag for multiple stints within stay based on bond amount ----
setorder(
  detentions_df,
  unique_identifier,
  detention_book_in_date_and_time,
  detention_facility_code,
  detention_release_reason,
  stay_book_out_date,
  stay_release_reason,
  religion,
  gender,
  marital_status,
  birth_year,
  ethnicity,
  entry_status,
  felon,
  bond_posted_date,
  bond_posted_amount,
  case_status,
  case_category,
  final_order_yes_no,
  final_order_date,
  case_threat_level,
  book_in_criminality,
  final_charge,
  departed_date,
  departure_country,
  citizenship_country,
  final_program,
  most_serious_conviction_charge_code,
  most_serious_conviction_charge
)

detentions_df[,
  likely_duplicate := {
    if (all(is.na(unique_identifier))) {
      rep(NA, .N)
    } else if (all(is.na(initial_bond_set_amount))) {
      seq_len(.N) != .N # tag all but the last row as TRUE
    } else {
      !seq_len(.N) %in%
        tail(
          which(
            initial_bond_set_amount ==
              min(initial_bond_set_amount, na.rm = TRUE)
          ),
          1
        )
    }
  },
  by = stint_ID
]

write_feather(detentions_df, "data/ice-final/detentions-final-with-flags.feather")
