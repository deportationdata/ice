# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)

# --- Read all arrests data --- 
# ROOT: ice/

# Read all .xlsx files (all sheets) under a directory and combine
dir = "data/ice-raw/arrests-selected"
pattern = "\\.xlsx$"
recursive = TRUE
guess_max = 10000
save_path = NULL

files <- list.files(path = dir, pattern = pattern, full.names = TRUE, recursive = recursive)

workbooks <- files |>
    set_names(basename(files)) |>
    purrr::map(function(f) {
        sheets <- readxl::excel_sheets(f)
        sheets |>
            set_names(sheets) |>
            purrr::map(~ readxl::read_excel(path = f, sheet = .x, guess_max = guess_max) |> janitor::clean_names() |> tibble::as_tibble())
    })

dfs_by_file <- workbooks |>
    purrr::map(~ dplyr::bind_rows(.x, .id = "sheet"))

new_dfs_by_file <- list()

for(i in seq_along(dfs_by_file)) {
    df <- dfs_by_file[[i]]
    file_name <- names(dfs_by_file)[i]
    cols <- colnames(df) # original column names before any processing
  
    # Find number of leading NAs in column x2
    n_NAs <- {
        x <- df$x2
        pos <- which(!is.na(x))
        if (length(pos) == 0) length(x) else pos[1] - 1
    } 
    
  
    new_cols <- df[n_NAs + 1, ] |> unlist(use.names = FALSE) |> as.character()
    new_df <- df[(n_NAs + 2):nrow(df), ]
    colnames(new_df) <- new_cols
  
    # first column = File name:
    colnames(new_df)[1] <- "file_name"
    final_cols <- colnames(new_df)[!is.na(colnames(new_df))]
    final_df <- new_df[, final_cols]
}
