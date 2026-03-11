rm(list=ls())

library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)

source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# list files in a directory
list_files_in_dir <- function(
  dir = "data/ice-raw/arrests-selected",
  pattern = "\\.xlsx$",
  recursive = TRUE
){
  files <- list.files(path = dir, pattern = pattern, full.names = TRUE, recursive = recursive)
  return(files)
}

find_first_non_na_row <- function(file_path, sheet, anchor_idx, guess_max = 10000) {
  raw_df <- read_excel(
    path = file_path,
    sheet = sheet,
    col_names = FALSE,
    guess_max = guess_max
  )

  if (anchor_idx < 1 || anchor_idx > ncol(raw_df)) {
    stop("anchor_idx is out of bounds for this sheet.")
  }

  anchor_vec <- raw_df %>% pull(anchor_idx)
  first_non_na_row <- which(!is.na(anchor_vec))[1]

  if (is.na(first_non_na_row)) {
    return(NA_integer_)
  }

  first_non_na_row
}

process_sheet <- function(file_path, sheet, anchor_idx, guess_max = 10000) {
  first_non_na_row <- find_first_non_na_row(
    file_path = file_path,
    sheet = sheet,
    anchor_idx = anchor_idx,
    guess_max = guess_max
  )

  if (is.na(first_non_na_row)) {
    return(tibble())
  }

  # Read just the header row
  header_df <- read_excel(
    path = file_path,
    sheet = sheet,
    range = cell_rows(first_non_na_row),
    col_names = FALSE
  )

  cleaned_names <- header_df %>%
    slice(1) %>%
    unlist(use.names = FALSE) %>%
    as.character() %>%
    str_replace_all("\\s+", "_") %>%
    make_clean_names(case = "none")

  # Read actual data below header row, using clean names
  data_df <- read_excel(
    path = file_path,
    sheet = sheet,
    skip = first_non_na_row,
    col_names = cleaned_names,
    guess_max = guess_max
  )

  data_df|>
    select(where(is_not_blank_or_redacted))

}
# read sheets from a file
read_sheets_from_file <- function(file_path, guess_max = 10000){
  sheet_names <- excel_sheets(file_path)
  return(sheet_names)
}

get_folder_df <- function(folder_dir, pattern, recursive, anchor_idx, guess_max){
  file_paths <- list_files_in_dir(
    dir = folder_dir,
    pattern = pattern,
    recursive = recursive
  )

  if (inherits(file_paths, "tbl_df") && "file_path" %in% names(file_paths)) {
    file_paths <- file_paths |> pull(file_path)
  }

  combined_df <- file_paths |>
    map_dfr(function(fp) {
      sheet_names <- read_sheets_from_file(fp)

      map_dfr(sheet_names, function(sh) {
        process_sheet(
          file_path = fp,
          sheet = sh,
          anchor_idx = anchor_idx,
          guess_max = 10000
        )
      })
    })

  combined_df <- combined_df |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))
  
  combined_df
}

get_file_df <- function(file_path, anchor_idx=2, guess_max = 10000){
  # Function for a file with multiple sheets 

  combined_df <- file_path |>
    map_dfr(function(fp) {
      sheet_names <- read_sheets_from_file(fp)

      map_dfr(sheet_names, function(sh) {
        process_sheet(
          file_path = fp,
          sheet = sh,
          anchor_idx = anchor_idx,
          guess_max = 10000
        )
      })
    })|>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))
  
  combined_df

}
