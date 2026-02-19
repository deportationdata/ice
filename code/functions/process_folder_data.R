library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)

rm(list=ls())
source("code/functions/convert_temporal_columns.R")

# list files in a directory
list_files_in_dir <- function(
  dir = "data/ice-raw/arrests-selected",
  pattern = "\\.xlsx$",
  recursive = TRUE
){
  files <- list.files(path = dir, pattern = pattern, full.names = TRUE, recursive = recursive)
  return(files)
}


# read sheets from a file
read_sheets_from_file <- function(file_path, guess_max = 10000){
  sheet_names <- excel_sheets(file_path)
  sheets <- map(sheet_names, ~ read_excel(file_path, sheet = .x, guess_max = guess_max))
  names(sheets) <- sheet_names
  return(sheets)
}

process_sheet<- function(sheet_df, anchor_idx){
  if (!is.numeric(anchor_idx) || length(anchor_idx) != 1) {
    stop("anchor_idx must be a single numeric column index.")
  }
  if (anchor_idx < 1 || anchor_idx > ncol(sheet_df)) {
    stop("anchor_idx is out of bounds for sheet_df.")
  }

  anchor_vec <- sheet_df %>% dplyr::pull(anchor_idx)

  first_non_na_row <- which(!is.na(anchor_vec))[1]
  if (is.na(first_non_na_row)) {
    return(tibble())
  }

  trimmed_df <- sheet_df %>%
    slice(first_non_na_row:n())

  cleaned_names <- trimmed_df %>%
    slice(1) %>%
    unlist(use.names = FALSE) %>%
    as.character() %>%
    str_replace_all("\\s+", "_") %>%
    make_clean_names(case = "none")

  trimmed_df %>%
    slice(-1) %>%
    set_names(cleaned_names)

}

get_folder_df <- function(folder_dir, pattern, recursive, anchor_idx, guess_max){
  file_paths <- list_files_in_dir(
    dir = folder_dir,
    pattern = pattern,
    recursive = recursive
  )

  # If list_files_in_dir() returns a tibble(file_path=...), handle that
  if (inherits(file_paths, "tbl_df") && "file_path" %in% names(file_paths)) {
    file_paths <- file_paths %>% pull(file_path)
  }

  combined_df <- file_paths %>%
    map_dfr(
      ~ read_sheets_from_file(.x) %>%
        imap_dfr(~ process_sheet(.x, anchor_idx = anchor_idx))
    )

  combined_df %>%
    convert_df_temporal_columns()
}