# Functions to inspect columns of a data frame
library(stringdist)
library(tibble)
library(dplyr)
library(purrr)


get_shared_cols <- function(df_list){
   column_names_by_df <- df_list %>%
    imap(~ names(.x))

  df_names <- names(column_names_by_df) %||% as.character(seq_along(column_names_by_df))

  shared_counts <- outer(
    column_names_by_df,
    column_names_by_df,
    FUN = Vectorize(function(a, b) length(intersect(a, b)))
  )

  dimnames(shared_counts) <- list(df_names, df_names)

  shared_counts
}

inspect_columns <- function(cols_in_A, cols_in_B) {

  shared_cols <- intersect(cols_in_A, cols_in_B)
  only_in_A   <- setdiff(cols_in_A, cols_in_B)
  only_in_B   <- setdiff(cols_in_B, cols_in_A)

  max_len <- max(length(shared_cols),
                 length(only_in_A),
                 length(only_in_B))

  pad_to <- function(x, n) {
    c(x, rep(NA_character_, n - length(x)))
  }

  tibble(
    shared_cols = pad_to(shared_cols, max_len),
    only_in_A   = pad_to(only_in_A,   max_len),
    only_in_B   = pad_to(only_in_B,   max_len)
  )
}


flag_near_matches <- function(cols_A, cols_B, max_dist = 2, method = "osa") {
  cols_A_unique <- unique(cols_A)
  cols_B_unique <- unique(cols_B)

  distance_matrix <- stringdistmatrix(cols_A_unique, cols_B_unique, method = method)

  best_match_index <- seq_along(cols_A_unique) %>%
    map_int(~ which.min(distance_matrix[.x, ]))

  best_distance <- seq_along(cols_A_unique) %>%
    map_dbl(~ distance_matrix[.x, best_match_index[.x]])

  tibble(
    col_A = cols_A_unique,
    best_match_in_B = cols_B_unique[best_match_index],
    distance = best_distance
  ) %>%
    mutate(flagged = distance <= max_dist) %>%
    arrange(distance, col_A)
}

