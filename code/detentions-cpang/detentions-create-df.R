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
  folder_dir = "data/ice-raw/detentions-selected/2019-ICFO-21307",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/detentions-selected/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df3 <- get_folder_df(
  folder_dir = "data/ice-raw/detentions-selected/2024-ICFO-41855",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df4 <- get_folder_df(
  folder_dir = "data/ice-raw/detentions-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df5 <- get_folder_df(
  folder_dir = "data/ice-raw/detentions-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)

df6 <- get_folder_df(
  folder_dir = "data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)
library(readxl)

# do separate processing for df7 since it's a CSV file
df7_raw <- read_csv("data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527/foia_10_2554_527_NoIDS.csv")
new_col_names <- colnames(df7_raw)|> str_replace_all("\\.", "_")
colnames(df7_raw) <- new_col_names

# parse date columns in df7
date_cols_df7 <- colnames(df7_raw)[grepl("(?i)_date", colnames(df7_raw))]
df7 <- df7_raw |>
  mutate(
    across(
      all_of(date_cols_df7),
      ~ as.Date(
          as.POSIXct(.x,
                     format = "%m/%d/%y %I:%M %p",
                     tz = "UTC")
        )
    )
  )

# --- Add source file column ---
df1 <- df1 %>% 
  mutate(source_file = "2019-ICFO-21307")

df2 <- df2 %>% 
  mutate(source_file = "2023_ICFO_42034")

df3 <- df3 %>% 
  mutate(source_file = "2024-ICFO-41855")

df4 <- df4 %>% 
  mutate(source_file = "120125")

df5 <- df5 %>% 
  mutate(source_file = "uwchr")

df6 <- df6 %>% 
  mutate(source_file = "From-Emily-Excel-X-RIF")

df7 <- df7 %>% 
  mutate(source_file = "From-Emily-FOIA-10-2554-527")

# --- Write out files ---
write_feather(df1, "data/ice-raw/detentions-selected/2019-ICFO-21307_combined.feather")
write_feather(df2, "data/ice-raw/detentions-selected/2023_ICFO_42034_combined.feather")
write_feather(df3, "data/ice-raw/detentions-selected/2024-ICFO-41855_combined.feather")
write_feather(df4, "data/ice-raw/detentions-selected/120125_combined.feather")
write_feather(df5, "data/ice-raw/detentions-selected/uwchr_combined.feather")
write_feather(df6, "data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF_combined.feather")
write_feather(df7, "data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527_combined.feather")
