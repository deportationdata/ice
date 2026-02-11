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

arrests_data <- arrests_data |>
  mutate(row_id = row_number()) # Create this row_id for 

na_counts <- arrests_data[, lapply(.SD, \(x) sum(is.na(x)))]
setcolorder(na_counts, names(na_counts)[order(as.numeric(na_counts[1]))])

arrests_data2 <- arrests_data |>
  arrange(Apprehension_Date)|>
  select(row_id,Apprehension_Date, Apprehension_Date_And_Time, source_file, Apprehension_Method, Gender, Citizenship_Country, Apprehension_AOR, Apprehension_Landmark)|>
  arrange(Apprehension_Date_And_Time)

match_cols <- c("Apprehension_Method","Gender","Citizenship_Country",
                "Apprehension_AOR","Apprehension_Landmark")

# Always-available date
arrests_data2[, app_date := as.IDate(Apprehension_Date)]  # works for "YYYY-mm-dd"; if not, tell me your format

# Datetime column that sometimes has just a date
x <- arrests_data2[["Apprehension_Date_And_Time"]]
x <- fifelse(is.na(x) | x == "", NA_character_, x)

arrests_data2[, app_dttm := as.POSIXct(
  x,
  tz = "UTC",
  tryFormats = c("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
)]

# Sort within blocks (date always, time when present)
setorderv(arrests_data2, c(match_cols, "app_date", "app_dttm"), na.last = TRUE)

# Compute "gap" to previous row within each block:
# - if both have timestamps -> hours gap
# - else -> day gap (proxy)
arrests_data2[, `:=`(
  prev_dttm = shift(app_dttm),
  prev_date = shift(app_date)
), by = match_cols]

arrests_data2[, gap_ok :=
  fifelse(
    !is.na(app_dttm) & !is.na(prev_dttm),
    as.numeric(difftime(app_dttm, prev_dttm, units = "hours")) <= 24,
    # fallback when either time missing: within ±1 day by date
    !is.na(prev_date) & abs(as.integer(app_date - prev_date)) <= 1L
  ),
  by = match_cols
]

# Start a new cluster when the gap is NOT ok (or first row in block)
arrests_data2[, dup_cluster_in_block := cumsum(is.na(gap_ok) | !gap_ok), by = match_cols]

# Create a readable ID (stable, interpretable)
arrests_data2[, dup_id := paste(
  Apprehension_Method, Gender, Citizenship_Country, Apprehension_AOR, Apprehension_Landmark,
  dup_cluster_in_block,
  sep = "|"
)]

# Flag clusters that have 2+ rows as likely duplicates
arrests_data2[, within_24h_flag := .N >= 2, by = dup_id]

# Optional: if you prefer dup_id only for flagged rows
arrests_data2[within_24h_flag == FALSE, dup_id := NA_character_]

# Cleanup helpers (optional)
arrests_data2[, c("prev_dttm","prev_date","gap_ok","dup_cluster_in_block") := NULL]

arrests_with_flags <- arrests_data[
  arrests_data2[, .(row_id, within_24h_flag, dup_id)],
  on = "row_id"
]

write_feather(arrests_with_flags, "data/ice-processed/arrests-merged-with-flags.feather")
