rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(tidyr)
library(data.table)
library(arrow)

detainers_data <- fread(
  "./data/ice-processed/detainers-merged.csv",
  colClasses = "character",
  na.strings = c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN"),
  showProgress = TRUE
)
detainers_data <- 
  detainers_data |>
  mutate(
    Prepare_Date = as.Date(Prepare_Date)
  )

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(detainers_data)
setorder(detainers_data, Unique_Identifier, Prepare_Date)

detainers_data[,
  `:=`(
    hours_since_last = as.numeric(
      Prepare_Date - shift(Prepare_Date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(Prepare_Date, type = "lead") - Prepare_Date,
      units = "hours"
    )
  ),
  by = Unique_Identifier
]

detainers_data <-
  detainers_data |>
  as_tibble() |>
  mutate(
    within_24hrs_prior = !is.na(hours_since_last) & hours_since_last <= 24,
    within_24hrs_next  = !is.na(hours_until_next) & hours_until_next <= 24,

    duplicate_likely = case_when(
      !is.na(Unique_Identifier) ~ within_24hrs_prior | within_24hrs_next,
      TRUE ~ FALSE
    ),

    # NEW: which rows to drop (drop the later row in a <=24hr pair)
    drop_row = case_when(
      is.na(Unique_Identifier) ~ FALSE,
      within_24hrs_prior ~ TRUE,   # later-than-previous within 24h => drop
      TRUE ~ FALSE
    )
  ) |>
  select(
    -within_24hrs_prior,
    -within_24hrs_next,
    -hours_since_last,
    -hours_until_next
  )

write_feather(detainers_data, "data/ice-processed/detainers-merged-with-flags.feather")
