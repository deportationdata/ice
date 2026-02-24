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
  folder_dir = "data/ice-raw/arrests-selected/2022-ICFO-22955",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2022-ICFO-22955")

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2023_ICFO_42034")

df3 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "120125")

df4 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  select(where(~ !all(is.na(.))))|>
  select(-NA_5)|>
  mutate(source_file = "uwchr")

# -- Save Combined Data ---
write_feather(df1, "data/ice-raw/arrests-selected/2022-ICFO-22955_combined.feather")
write_feather(df2, "data/ice-raw/arrests-selected/2023_ICFO_42034_combined.feather")
write_feather(df3, "data/ice-raw/arrests-selected/120125_combined.feather")
write_feather(df4, "data/ice-raw/arrests-selected/uwchr_combined.feather")
