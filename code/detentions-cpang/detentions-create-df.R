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
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/check_dttm_and_convert_to_date.R")

# --- Read all arrests data --- 
# ROOT: ice/
df1 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/2019-ICFO-21307",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2019-ICFO-21307")

df1 <- df1 |>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df2 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2023_ICFO_42034")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df3 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/2024-ICFO-41855",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2024-ICFO-41855")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df3 <- df3 |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))

df4 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "120125")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df5 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "uwchr")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df6 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "From-Emily-Excel-X-RIF")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

library(readxl)
library(janitor)

# do separate processing for df7 since it's a CSV file
df7_raw <- read_csv("data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527/foia_10_2554_527_NoIDS.csv")
df7_raw <- df7_raw |> 
  clean_names(case= "upper_camel")|>
  rename_with(~ str_replace_all(.x, "(?<=[a-z])(?=[A-Z])", "_"))

# parse date columns in df7
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
  )|>
  mutate(source_file = "From-Emily-FOIA-10-2554-527")|>
  mutate(across(ends_with("_Date"), ~ as.Date(mdy_hm(.x))))

df8 <- get_folder_df0(
  folder_dir = "data/ice-raw/detentions-selected/November 2025 Release",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "November 2025 Release")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

# --- Write out files ---
write_feather(df1, "data/ice-raw/detentions-selected/2019-ICFO-21307_combined.feather")
write_feather(df2, "data/ice-raw/detentions-selected/2023_ICFO_42034_combined.feather")
write_feather(df3, "data/ice-raw/detentions-selected/2024-ICFO-41855_combined.feather")
write_feather(df4, "data/ice-raw/detentions-selected/120125_combined.feather")
write_feather(df5, "data/ice-raw/detentions-selected/uwchr_combined.feather")
write_feather(df6, "data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF_combined.feather")
write_feather(df7, "data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527_combined.feather")
write_feather(df8, "data/ice-raw/detentions-selected/nov2025_combined.feather")
