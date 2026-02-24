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

# --- Read all arrests data --- 
# ROOT: ice/
df1 <- get_folder_df(
  folder_dir = "data/ice-raw/removals-selected/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2023_ICFO_42034")

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/removals-selected/082025",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "082025")

df3 <- get_folder_df(
  folder_dir = "data/ice-raw/removals-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "uwchr")

# --- df4 needs separate processing --- 
get_folder_df2 <- function(folder_dir, pattern, recursive, anchor_idx, sheet_n = 2){
  files <- list_files_in_dir(dir = folder_dir, pattern = pattern, recursive = recursive) #df4_files 
  folder_df <- tibble::tibble()
  for (f in files){ # for the 1st file
    sheets <- read_sheets_from_file(f, guess_max = 10000) # read in all sheets 
    sheet_df <- sheets[[sheet_n]] # read in the 2nd sheet 
    processed_df <- process_sheet(sheet_df, anchor_idx = anchor_idx)
    folder_df <- bind_rows(folder_df, processed_df)
    print(head(processed_df))
    
  }
  # Convert temporal columns
  folder_df2 <- convert_df_temporal_columns(folder_df)
  return(folder_df2)
}

df4 <- get_folder_df2(
  folder_dir = "data/ice-raw/removals-selected/14-03290",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2, 
  sheet_n = 2
)|>
  mutate(source_file = "14-03290")

# --- Write out files ---
write_feather(df1, "data/ice-raw/removals-selected/2023_ICFO_42034_combined.feather")
write_feather(df2, "data/ice-raw/removals-selected/082025_combined.feather")
write_feather(df3, "data/ice-raw/removals-selected/uwchr_combined.feather")
write_feather(df4, "data/ice-raw/removals-selected/14-03290_combined.feather")
