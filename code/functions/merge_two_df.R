library(dplyr)
library(purrr)
library(tibble)


# Assumes inspect_columns() is available from your sourced file
source("code/functions/inspect_columns.R")

merge_dfs <- function(df1, df2, df1_cols_old, df1_cols_new, df2_cols_old, df2_cols_new) {

  # Build named lookup vectors: names are new names, values are old names
  lookup_df1 <- stats::setNames(df1_cols_old, df1_cols_new)
  lookup_df2 <- stats::setNames(df2_cols_old, df2_cols_new)

  # Rename columns using any_of() so missing columns do not error
  df1 <- df1 %>%
    dplyr::rename(dplyr::any_of(lookup_df1))

  df2 <- df2 %>%
    dplyr::rename(dplyr::any_of(lookup_df2))

  # Coerce shared columns to same type if needed
  common_cols <- intersect(names(df1), names(df2))

  for (col in common_cols) {
    if (!identical(class(df1[[col]]), class(df2[[col]]))) {
      df1[[col]] <- as.character(df1[[col]])
      df2[[col]] <- as.character(df2[[col]])
    }
  }

  venn_after <- inspect_columns(names(df1), names(df2))
  df_merged <- dplyr::bind_rows(df1, df2)

  list(
    df1_renamed = df1,
    df2_renamed = df2,
    venn_after = venn_after,
    df_merged = df_merged
  )
}