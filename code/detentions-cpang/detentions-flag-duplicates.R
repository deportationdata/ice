rm(list=ls())
gc()

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(tidyr)
library(data.table)
library(arrow)

detentions_data <- read_feather("data/ice-processed/detentions-merged.feather")
detentions_data <- 
  detentions_data |>
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date)
  )

# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(detentions_data)
setorder(detentions_data, Unique_Identifier, Detention_Book_In_Date)

detentions_data[,
  `:=`(
    hours_since_last = as.numeric(
      Detention_Book_In_Date - shift(Detention_Book_In_Date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(Detention_Book_In_Date, type = "lead") - Detention_Book_In_Date,
      units = "hours"
    )
  ),
  by = Unique_Identifier
]

detentions_data <-
  detentions_data |>
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

write_feather(detentions_data, "data/ice-processed/detentions-merged-with-flags.feather")
