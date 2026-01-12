library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)

create_metadata <- function(dir = "data/ice-raw/arrests-selected", pattern = "\\.xlsx$", recursive = TRUE, guess_max = 10000, save_path = NULL){
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
  meta_by_file <- list()

  for (i in seq_along(dfs_by_file)) {
  df <- dfs_by_file[[i]]
  file_name <- names(dfs_by_file)[i]

  cols_original <- colnames(df) # original column names before any processing

  # Find number of leading NAs in column x2
  n_NAs <- {
    x <- df$x2
    pos <- which(!is.na(x))
    if (length(pos) == 0) length(x) else pos[1] - 1
  }

  # If the file is "all NA in x2", you can decide how to handle it
  # (this keeps it from crashing later)
  if (n_NAs >= nrow(df) - 1) {
    new_dfs_by_file[[file_name]] <- tibble::tibble()
    meta_by_file[[file_name]] <- list(
      file_name = file_name,
      status = "skipped (x2 all NA or not enough rows)",
      cols_original = cols_original,
      n_leading_NAs_x2 = n_NAs,
      n_rows_raw = nrow(df),
      n_cols_raw = ncol(df)
    )
    next
  }

  # Row n_NAs + 1 is your header row
  header_row_raw <- df[n_NAs + 1, ] |>
    unlist(use.names = FALSE) |>
    as.character()

  new_df <- df[(n_NAs + 2):nrow(df), ]
  colnames(new_df) <- header_row_raw

  # first column = File name:
  colnames(new_df)[1] <- "file_name"

  # drop NA column names
  final_cols <- colnames(new_df)[!is.na(colnames(new_df)) & colnames(new_df) != ""]
  final_cols_clean <- gsub(" ", "_", final_cols) # replace spaces with underscores

  final_df <- new_df[, final_cols, drop = FALSE]
  colnames(final_df) <- final_cols_clean
  # store processed df
  new_dfs_by_file[[file_name]] <- final_df

  # store metadata
  meta_by_file[[file_name]] <- list(
    file_name = file_name,
    processed_df = final_df,
    col_names = colnames(final_df)
  )
  }

  saveRDS(meta_by_file, save_path)
  return(meta_by_file)

}