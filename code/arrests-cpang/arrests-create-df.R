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
  folder_dir = "data/ice-raw/arrests-selected/2022-ICFO-22955",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df3 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df4 <- get_folder_df(
  folder_dir = "data/ice-raw/arrests-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
) # need to remove columns with all NAs 

df4 <- df4[, -((ncol(df4)-4):ncol(df4))]

# --- Add "source_file" Column ---
df1$source_file <- "2022-ICFO-22955"
df2$source_file <- "2023_ICFO_42034"
df3$source_file <- "120125"
df4$source_file <- "uwchr"

# -- Save Combined Data ---
write.csv(df1, "data/ice-raw/arrests-selected/2022-ICFO-22955_combined.csv", row.names = FALSE)
write.csv(df2, "data/ice-raw/arrests-selected/2023_ICFO_42034_combined.csv", row.names = FALSE)
write.csv(df3, "data/ice-raw/arrests-selected/120125_combined.csv", row.names = FALSE)
write.csv(df4, "data/ice-raw/arrests-selected/uwchr_combined.csv", row.names = FALSE)
