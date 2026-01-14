# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)

source("code/functions/process_folder_data.R")
# --- Read all arrests data --- 
# ROOT: ice/
icfo22955 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/2022-ICFO-22955",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

icfo42034 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

f120125 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

uwchr <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
) # need to remove columns with all NAs 
