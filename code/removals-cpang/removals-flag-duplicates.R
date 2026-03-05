rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(tidyr)
library(data.table)
library(arrow)

removals_data <- read_feather("data/ice-processed/removals-merged.feather")


removals_data <- 
  removals_data |>
  mutate(
    Departed_Date = as.Date(Departed_Date)
  )

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(removals_data)
setorder(removals_data, Unique_Identifier, Departed_Date)

removals_data[,
  `:=`(
    hours_since_last = as.numeric(
      Departed_Date - shift(Departed_Date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(Departed_Date, type = "lead") - Departed_Date,
      units = "hours"
    )
  ),
  by = Unique_Identifier
]

removals_data <-
  removals_data |>
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

write_feather(removals_data, "data/ice-processed/removals-merged-with-flags.feather")
