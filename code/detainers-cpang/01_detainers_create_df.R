# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readr)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)
library(arrow)

# --- Source Functions ---
source("code/functions/process_folder_data.R")
source("code/functions/is_not_blank_or_redacted.R")

# --- Read all arrests data --- 
# ROOT: ice/
# use the OLD folder parser that doesn't guess and converts everything to character and then guess the columns later... 
df1 <- get_folder_df0(
  folder_dir = "data/ice-raw/detainers-selected/2025-ICFO-18038",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2025-ICFO-18038")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

source("code/functions/process_folder_data_v2.R")

df2 <- get_folder_df(
  folder_dir = "data/ice-raw/detainers-selected/120125",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "120125")|>
  select(where(is_not_blank_or_redacted))

# --- For npr, there are .csv and .txt files so get_folder_df won't work ---
npr_csv_files <- list.files(
  path = "data/ice-raw/detainers-selected/npr",
  pattern = "\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)
npr_txt_files <- list.files(
  path = "data/ice-raw/detainers-selected/npr",
  pattern = "\\.txt$",
  recursive = TRUE,
  full.names = TRUE
)

# ---------- Helpers ----------
read_npr_delim <- function(path) {
  # You used read.delim() for these "csv" files, so keep that behavior.
  # read_tsv() matches read.delim() defaults (tab-delimited).
  read_tsv(
    file = path,
    col_types = cols(.default = col_character()),
    name_repair = "minimal"
  ) |>
    rename_with(~ str_replace_all(.x, "\\.", "_"))
}

read_npr_csv <- function(path) {
  read_csv(
    file = path,
    col_types = cols(.default = col_character()),
    name_repair = "minimal"  # similar to check.names = FALSE (don't mangle names)
  ) |>
    rename_with(~ str_replace_all(.x, " ", "_"))
}

parse_date_like <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, c("POSIXct", "POSIXlt"))) return(as.Date(x))

  x_chr <- str_trim(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN")] <- NA_character_

  as.Date(
    x_chr,
    tryFormats = c(
      "%Y-%m-%d",  # ISO
      "%m/%d/%y",  # 2-digit year first (prevents 0009 issue)
      "%m/%d/%Y"   # 4-digit year
    )
  )
}

batch_to_date <- function(df, cols, verbose = TRUE) {
  cols <- intersect(cols, names(df))

  out <- df |>
    mutate(across(all_of(cols), parse_date_like))

  if (verbose && length(cols) > 0) {
    walk(cols, \(col) {
      converted_n <- sum(!is.na(out[[col]]))
      cat(sprintf(
        "Column: %-35s | Converted: %7d / %7d\n",
        col, converted_n, nrow(out)
      ))
    })
  }

  out
}

# ---------- Read NPR files ----------
df3 <- map_dfr(npr_csv_files, read_npr_delim)
# Equivalent of npr_txt_files[1:5] + separate read of [6]
df4 <- map_dfr(npr_txt_files[1:5], read_npr_csv)
df5 <- read_npr_csv(npr_txt_files[6])

# Merge df3, df4, df5 since these are all from npr
npr_merge <- bind_rows(df3, df4, df5)

# ---------- Date conversion ----------
date_cols <- names(npr_merge) |>
  keep(\(nm) stringr::str_detect(nm, stringr::regex("Date", ignore_case = TRUE))) |>
  setdiff(c("Birth_Date", "Birth Date"))

npr_merge_final <- batch_to_date(npr_merge, date_cols)

npr_merge_final <- npr_merge_final |>
  rename_with(\(nms) {
    nms |>
      str_replace_all("\\s+", "_") |>
      make_clean_names(case = "none") |>
      str_replace_all("_+", "_") |>
      str_split("_") |>
      lapply(\(parts) paste(str_to_title(str_to_lower(parts)), collapse = "_")) |>
      unlist()
  })|>
  mutate(source_file = "npr")

df_nov2025 <- get_folder_df(
  folder_dir = "data/ice-raw/detainers-selected/November 2025 Release",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "November 2025 Release")|>
  select(where(is_not_blank_or_redacted))

write_feather(df1, "data/ice-raw/detainers-selected/2025-ICFO-18038_combined.feather")
write_feather(df2, "data/ice-raw/detainers-selected/120125_combined.feather")
write_feather(npr_merge_final, "data/ice-raw/detainers-selected/npr_combined.feather")
write_feather(df_nov2025, "data/ice-raw/detainers-selected/nov2025_combined.feather")
