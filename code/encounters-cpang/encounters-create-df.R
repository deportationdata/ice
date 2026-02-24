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
source("code/functions/process_folder_data.R")
source("code/functions/inspect_columns.R")

# --- Read all arrests data --- 
# ROOT: ice/
df1 <- get_folder_df(
  folder_dir = "data/ice-raw/encounters-selected/082025",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "082025")

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/encounters-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "uwchr")

# --- Write out files ---
write_feather(df1, "data/ice-raw/encounters-selected/f082025_combined.feather")
write_feather(df2, "data/ice-raw/encounters-selected/uwchr_combined.feather")
