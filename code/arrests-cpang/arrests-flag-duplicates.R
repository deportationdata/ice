rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(tidyr)
library(data.table)
library(arrow)

arrests_data <- fread(
  "./data/ice-processed/arrests-merged.csv",
  colClasses = "character",
  na.strings = c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN"),
  showProgress = TRUE
)

arrests_data <- 
  arrests_data |>
  mutate(
    # convert apprehension date to as.Date() object
    Apprehension_Date = as.Date(Apprehension_Date)
  )

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(arrests_data)
setorder(arrests_data, Unique_Identifier, Apprehension_Date)

arrests_data[,
  `:=`(
    hours_since_last = as.numeric(
      Apprehension_Date - shift(Apprehension_Date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(Apprehension_Date, type = "lead") - Apprehension_Date,
      units = "hours"
    )
  ),
  by = Unique_Identifier
]


arrests_data <-
  arrests_data |>
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

write_feather(arrests_data, "data/ice-processed/arrests-merged-with-flags.feather")
