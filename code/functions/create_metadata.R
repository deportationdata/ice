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

  if (!is.null(save_path)){
    saveRDS(meta_by_file, save_path)
  }
  
  return(meta_by_file)

}

to_date <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, c("POSIXct","POSIXlt"))) return(as.Date(x))

  x_chr <- trimws(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  x_chr <- gsub(",", "", x_chr)

  x_num <- suppressWarnings(as.numeric(x_chr))
  is_excel <- !is.na(x_num) & x_num > 20000 & x_num < 60000

  out <- rep(as.Date(NA), length(x_chr))
  out[is_excel] <- as.Date(x_num[is_excel], origin = "1899-12-30")

  rem <- is.na(out) & !is.na(x_chr)
  if (any(rem)) {
    fmts <- c("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d")
    parsed <- rep(as.Date(NA), sum(rem))
    for (f in fmts) {
      tmp <- suppressWarnings(as.Date(x_chr[rem], format = f))
      parsed[is.na(parsed) & !is.na(tmp)] <- tmp[is.na(parsed) & !is.na(tmp)]
    }
    out[rem] <- parsed
  }

  out
}

fix_metadata_dates <- function(metadata){
  new_metadata_list <- list()
  for (i in seq_along(metadata)) {
    cols <- metadata[[i]]$col_names

    time_related_cols <- cols |>
      str_subset(regex("(^|_)(date|datetime|date_time|time|timestamp|year|month|day|hour|minute|min|second|sec)($|_)",
                      ignore_case = TRUE))
    # split time-related cols into Date, Year, or String
    date_related_cols <- time_related_cols |>
      str_subset(regex("(^|_)(date|datetime|date_time|time|timestamp|month|day|hour|minute|min|second|sec)($|_)",
                      ignore_case = TRUE))
    year_related_cols <- 
    
    
    
    cat("File:", arrests_metadata[[i]]$file_name, "\n")
    #cat("Time-related cols:", paste(time_related_cols, collapse = ", "), "\n\n")

    df <- arrests_metadata[[i]]$processed_df
    print(head(df[,time_related_cols]))

    for(col in time_related_cols) {
      if(col %in% date_related_cols){
        converted <- to_date(df[[col]])
      }

      
      num_converted <- sum(!is.na(converted))
      cat("Column:", col, "- Converted to Date:", num_converted, "out of", nrow(df), "\n")
      df[[col]] <- converted
    }

    new_metadata_list[[i]] <- arrests_metadata[[i]]
    new_metadata_list[[i]]$processed_df <- df
  }
  return(new_metadata_list)
}

merge_df_by_folder <- function(meta_by_file){
  all_dfs <- lapply(meta_by_file, function(x) x$processed_df)
  merged_df <- dplyr::bind_rows(all_dfs)
  return(merged_df)
}