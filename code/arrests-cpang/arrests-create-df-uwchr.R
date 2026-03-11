# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)
library(arrow)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")

folder_dir = "data/ice-raw/arrests-selected/uwchr"
pattern = "\\.xlsx$"
recursive = TRUE
anchor_idx = 2
guess_max = 10000



files <- list_files_in_dir(folder_dir, pattern, recursive)
sheet_name_list <- list()
idx <- 1 

for(fp in files){
  print(paste0("Reading file path: ", fp))
  sheet_names <- read_sheets_from_file(fp, guess_max)
  for(sh in sheet_names){
    print(paste0("Sheet: ", sh))
    sheet_df <- process_sheet(fp, sh, anchor_idx, guess_max)
    sheet_name_list[[idx]] <- names(sheet_df)
    idx <- idx + 1
  }
}

