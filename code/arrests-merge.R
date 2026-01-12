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


# --- Read all arrests data --- 
# ROOT: ice/

# Read all .xlsx files (all sheets) under a directory and combine
dir = "data/ice-raw/arrests-selected"
pattern = "\\.xlsx$"
recursive = TRUE
guess_max = 10000
save_path = "data/ice-processed/arrests-selected-meta-by-file.rds"

arrests_metadata <- create_metadata(dir = dir, pattern = pattern, recursive = recursive, guess_max = guess_max, save_pa
#arrests_metadata <- readRDS("data/ice-processed/arrests-selected-meta-by-file.rds")
arrests_metadata_dates <- fix_metadata_dates(arrests_metadata)
