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
  folder_dir = "data/ice-raw/encounters-selected/082025",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/encounters-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df1$source_file <- "082025"
df2$source_file <- "uwchr"

# --- Write out files ---
write.csv(df1, "data/ice-raw/encounters-selected/f082025_combined.csv", row.names = FALSE)
write.csv(df2, "data/ice-raw/encounters-selected/uwchr_combined.csv", row.names = FALSE)
