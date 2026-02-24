rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(tidyr)
library(data.table)
library(arrow)

encounters_data <- read_feather("data/ice-processed/encounters-merged.feather")


encounters_data <- 
  encounters_data |>
  mutate(
    Event_Date = as.Date(Event_Date)
  )

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(encounters_data)
setorder(encounters_data, Unique_Identifier, Event_Date)

encounters_data[,
  `:=`(
    hours_since_last = as.numeric(
      Event_Date - shift(Event_Date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(Event_Date, type = "lead") - Event_Date,
      units = "hours"
    )
  ),
  by = Unique_Identifier
]

encounters_data <-
  encounters_data |>
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

write_feather(encounters_data, "data/ice-processed/encounters-merged-with-flags.feather")
