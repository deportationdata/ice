library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)

rm(list=ls())
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
read_sheets_from_file <- function(file_path,guess_max = 10000){
  sheet_names <- excel_sheets(file_path)
  sheets <- map(sheet_names, ~ read_excel(file_path, sheet = .x, guess_max = guess_max))
  names(sheets) <- sheet_names
  return(sheets)
}

process_sheet<- function(df, anchor_idx){
  
  # helper function for counting leading NAs
  count_leading_na <- function(x) {
    pos <- which(!is.na(x))
    if (length(pos) == 0) length(x) else pos[1] - 1
  }
  n_NAs <- count_leading_na(df[[anchor_idx]])
  # remove leading NAs 
  df_remove_na <- df[(n_NAs + 1):nrow(df), ]

  # set first row as column names and add underscores
  new_col_names <- as.character(df_remove_na[1, ])|> str_replace_all(" ", "_")
  
  # drop first row
  new_df <- df_remove_na[-1, ]
  colnames(new_df) <- new_col_names

  return(new_df)

}

get_folder_df <- function(folder_dir, pattern, recursive, anchor_idx){
  files <- list_files_in_dir(dir = folder_dir, pattern = pattern, recursive = recursive)
  folder_df <- tibble::tibble()
  for (f in files){
    sheets <- read_sheets_from_file(f, guess_max = 10000)
    for (s in names(sheets)){
      sheet_df <- sheets[[s]]
      processed_df <- process_sheet(sheet_df, anchor_idx = anchor_idx)
      folder_df <- bind_rows(folder_df, processed_df)
      print(head(processed_df))
    }
  }
  return(folder_df)
}