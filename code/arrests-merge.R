# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)

source("code/functions/create_metadata.R")
source("code/functions/guess_column_types_across_sheets.R")
source("code/functions/check_dttm_and_convert_to_date.R")
# --- Read all arrests data --- 
# ROOT: ice/

# Read all .xlsx files (all sheets) under a directory and combine
# dir = "data/ice-raw/arrests-selected"
# pattern = "\\.xlsx$"
# recursive = TRUE
# guess_max = 10000
# save_path = "data/ice-processed/arrests-selected-meta-by-file.rds"

# arrests_metadata <- create_metadata(dir = dir, pattern = pattern, recursive = recursive, guess_max = guess_max, save_path = save_path)
arrests_metadata <- readRDS("data/ice-processed/arrests-selected-meta-by-file.rds")

to_date <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, c("POSIXct","POSIXlt"))) return(as.Date(x))

  x_chr <- trimws(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  x_chr <- gsub(",", "", x_chr)

  x_num <- suppressWarnings(as.numeric(x_chr))
  is_excel <- !is.na(x_num) & x_num > 20000 & x_num < 60000

  out <- rep(as.Date(NA), length(x_chr))
  out[is_excel] <- as.Date(x_num[is_excel], origin = "1899-12-30")

  rem <- is.na(out) & !is.na(x_chr)
  if (any(rem)) {
    fmts <- c("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d")
    parsed <- rep(as.Date(NA), sum(rem))
    for (f in fmts) {
      tmp <- suppressWarnings(as.Date(x_chr[rem], format = f))
      parsed[is.na(parsed) & !is.na(tmp)] <- tmp[is.na(parsed) & !is.na(tmp)]
    }
    out[rem] <- parsed
  }

  out
}



new_metadata_list <- list()
for (i in seq_along(arrests_metadata)) {
  cols <- arrests_metadata[[i]]$col_names

  time_related_cols <- cols |>
    str_subset(regex("(^|_)(date|datetime|date_time|time|timestamp|year|month|day|hour|minute|min|second|sec)($|_)",
                     ignore_case = TRUE))

  cat("File:", arrests_metadata[[i]]$file_name, "\n")
  #cat("Time-related cols:", paste(time_related_cols, collapse = ", "), "\n\n")

  df <- arrests_metadata[[i]]$processed_df
  print(head(df[,time_related_cols]))

  for(col in time_related_cols) {
    converted <- to_date(df[[col]])
    num_converted <- sum(!is.na(converted))
    cat("Column:", col, "- Converted to Date:", num_converted, "out of", nrow(df), "\n")
    df[[col]] <- converted
  }
  new_metadata_list[[i]] <- arrests_metadata[[i]]
  new_metadata_list[[i]]$processed_df <- df
}

