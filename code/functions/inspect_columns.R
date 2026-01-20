# Functions to inspect columns of a data frame

library(stringdist)
library(tibble)
library(dplyr)

get_shared_cols <- function(df_list){
  # generates pair-wise matrix of shared columns between data frames in a list
  # Use this function to inspect columns to merge
  cols <- lapply(df_list, names)

  # pairwise counts of shared columns
  shared_n <- outer(cols, cols, Vectorize(function(a, b) length(intersect(a, b))))
  dimnames(shared_n) <- list(names(df), names(df))
  
  return(shared_n)
}

inspect_columns <- function(cols_in_A, cols_in_B) {
  shared_cols <- intersect(cols_in_A, cols_in_B)
  only_in_A   <- setdiff(cols_in_A, cols_in_B)
  only_in_B   <- setdiff(cols_in_B, cols_in_A)

  max_len <- max(length(shared_cols), length(only_in_A), length(only_in_B))

  pad <- function(x, n) c(x, rep(NA_character_, n - length(x)))

  venn_diagram_df <- data.frame(
    shared_cols = pad(shared_cols, max_len),
    only_in_A   = pad(only_in_A,   max_len),
    only_in_B   = pad(only_in_B,   max_len),
    stringsAsFactors = FALSE
  )
  return(venn_diagram_df)
}

flag_near_matches <- function(cols_A, cols_B, max_dist = 2, method = "osa") {
  A <- unique(cols_A)
  B <- unique(cols_B)

  # distance matrix (rows = A, cols = B)
  D <- stringdistmatrix(A, B, method = method)

  best_j <- apply(D, 1, which.min)
  best_d <- D[cbind(seq_along(A), best_j)]

  tibble(
    col_A = A,
    best_match_in_B = B[best_j],
    distance = best_d,
    flagged = best_d <= max_dist
  ) %>%
    arrange(distance, col_A)
}

