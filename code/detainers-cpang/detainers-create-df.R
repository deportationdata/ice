# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)

# --- Source Functions ---
source("code/functions/process_folder_data.R")
source("code/functions/inspect_columns.R")
source("code/functions/convert_temporal_columns.R")

# --- Read all arrests data --- 
# ROOT: ice/
df1 <- get_folder_df(
  folder_dir = "data/ice-raw/detainers-selected/2025-ICFO-18038",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/detainers-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

# --- For npr, there are .csv and .txt files so get_folder_df won't work ---
npr_csv_files <- list.files(
  path = "data/ice-raw/detainers-selected/npr",
  pattern = "\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)
npr_txt_files <- list.files(
  path = "data/ice-raw/detainers-selected/npr",
  pattern = "\\.txt$",
  recursive = TRUE,
  full.names = TRUE
)

df3 <- do.call(rbind, lapply(npr_csv_files, function(f) {

  df <- read.delim(
    f,
    stringsAsFactors = FALSE,
    header = TRUE
  )
  df
}))
names(df3) <- gsub("\\.", "_", names(df3))


df4 <- do.call(rbind, lapply(npr_txt_files[1:5], function(f) {

  df <- read.csv(
    f,
    stringsAsFactors = FALSE,
    header = TRUE,
    check.names = FALSE
  )

  
}))
names(df4) <- gsub(" ", "_", names(df4))

df5 <- read.csv(
    npr_txt_files[6],
    stringsAsFactors = FALSE,
    header = TRUE,
    check.names = FALSE
  )
names(df5) <- gsub(" ", "_", names(df5))

# Merge df3,4, and 5 since these are all from npr 
npr_merge <- bind_rows(df3, df4, df5)

batch_to_date <- function(df, cols, verbose = TRUE) {
  cols <- intersect(cols, names(df))

  parse_one <- function(x) {
    if (inherits(x, "Date")) return(x)
    if (inherits(x, c("POSIXct", "POSIXlt"))) return(as.Date(x))

    x_chr <- trimws(as.character(x))
    x_chr[x_chr %in% c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN")] <- NA_character_

    out <- as.Date(x_chr, tryFormats = c(
      "%Y-%m-%d",  # ISO
      "%m/%d/%y",  # <-- 2-digit year FIRST (prevents 0009)
      "%m/%d/%Y"   # 4-digit year
    ))
    out
  }

  for (col in cols) {
    converted <- parse_one(df[[col]])
    if (verbose) {
      cat(sprintf("Column: %-35s | Converted: %7d / %7d\n",
                  col, sum(!is.na(converted)), length(converted)))
    }
    df[[col]] <- converted
  }

  df
}


date_cols <- setdiff(names(npr_merge)[grepl("Date", names(npr_merge), ignore.case = TRUE)], "Birth_Date")
npr_merge_final <- batch_to_date(npr_merge, date_cols)

# --- All Field "source_file" Column ---
df1$source_file <- "2025-ICFO-18038"
df2$source_file <- "120125"
npr_merge_final$source_file <- "npr"

# --- Write out Combined Data ---
write.csv(df1, "data/ice-raw/detainers-selected/2025-ICFO-18038_combined.csv", row.names = FALSE)
write.csv(df2, "data/ice-raw/detainers-selected/120125_combined.csv", row.names = FALSE)
write.csv(npr_merge_final, "data/ice-raw/detainers-selected/npr_combined.csv", row.names = FALSE)
