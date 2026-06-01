library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)

source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

find_first_non_na_row <- function(
  file_path,
  sheet,
  anchor_idx,
  guess_max = 10000
) {
  raw_df <- read_excel(
    path = file_path,
    sheet = sheet,
    col_names = FALSE,
    guess_max = guess_max
  )

  if (anchor_idx < 1 || anchor_idx > ncol(raw_df)) {
    stop("anchor_idx is out of bounds for this sheet.")
  }

  anchor_vec <- raw_df |> pull(anchor_idx)
  first_non_na_row <- which(!is.na(anchor_vec))[1]

  if (is.na(first_non_na_row)) {
    return(NA_integer_)
  }

  first_non_na_row
}

all_text <- function(cols) setNames(rep("text", length(cols)), cols)

process_sheet <- function(
  file_path,
  sheet,
  anchor_idx,
  col_type_overrides,
  guess_max = 10000
) {
  first_non_na_row <- find_first_non_na_row(
    file_path = file_path,
    sheet = sheet,
    anchor_idx = anchor_idx,
    guess_max = guess_max
  )

  if (is.na(first_non_na_row)) {
    return(tibble())
  }

  header_df <- read_excel(
    path = file_path,
    sheet = sheet,
    range = cell_rows(first_non_na_row),
    col_names = FALSE
  )

  cleaned_names <-
    header_df |>
    slice(1) |>
    unlist(use.names = FALSE) |>
    as.character() |>
    str_replace_all("\\s+", "_") |>
    make_clean_names(case = "none")

  missing <- setdiff(cleaned_names, names(col_type_overrides))
  if (length(missing) > 0) {
    stop(sprintf(
      "process_sheet: %s :: %s has columns not covered by col_type_overrides: %s",
      basename(file_path),
      sheet,
      paste(missing, collapse = ", ")
    ))
  }

  col_types <- unname(col_type_overrides[cleaned_names])

  data_df <- read_excel(
    path = file_path,
    sheet = sheet,
    skip = first_non_na_row,
    col_names = FALSE,
    col_types = col_types,
    guess_max = guess_max
  )

  n_keep <- min(ncol(data_df), length(cleaned_names))

  data_df |>
    select(all_of(seq_len(n_keep))) |>
    set_names(cleaned_names[seq_len(n_keep)])
}
