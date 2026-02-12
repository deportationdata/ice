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



detainers_data <- detainers_data |>
  mutate(row_id = row_number()) # Create this row_id for 
npr_detentions <- detainers_data |>
  filter(source_file == "npr")
na_counts <- detainers_data[, lapply(.SD, \(x) sum(is.na(x)))]
setcolorder(na_counts, names(na_counts)[order(as.numeric(na_counts[1]))])

temp <- detainers_data |>
  arrange(Prepare_Date) |>
  select(row_id, Prepare_Date, source_file, Detention_Facility, Detainer_Lift_Reason, Detention_Facility_Code, Detainer_AOR, Birth_Country, Gender, Birth_Year)

match_cols <- c("Detention_Facility", "Detainer_Lift_Reason", "Detention_Facility_Code", "Detainer_AOR", "Birth_Country", "Gender", "Birth_Year")

# Always-available date
temp[, app_date := as.IDate(Prepare_Date)]  # works for "YYYY-mm-dd"; if not, tell me your format

# Sort within blocks (date always, time when present)
setorderv(temp, c(match_cols, "app_date"), na.last = TRUE)

# Compute "gap" to previous row within each block:
# - if both have timestamps -> hours gap
# - else -> day gap (proxy)
temp[, `:=`(
  prev_date = shift(app_date)
), by = match_cols]


temp[, gap_ok :=# fallback when either time missing: within ±1 day by date
    !is.na(prev_date) & abs(as.integer(app_date - prev_date)) <= 1L
  ,
  by = match_cols
]
# Start a new cluster when the gap is NOT ok (or first row in block)
temp[, dup_cluster_in_block := cumsum(is.na(gap_ok) | !gap_ok), by = match_cols]

# Create a readable ID (stable, interpretable)
temp[, dup_id := paste(
  Apprehension_Method, Gender, Citizenship_Country, Apprehension_AOR, Apprehension_Landmark,
  dup_cluster_in_block,
  sep = "|"
)]