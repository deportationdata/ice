library(dplyr)
library(purrr)
library(tibble)

# Assumes inspect_columns() is available from your sourced file
source("code/functions/inspect_columns.R")

merge_dfs <- function(
  df1, df2,
  df1_cols_old, df1_cols_new,
  df2_cols_old, df2_cols_new,
  verbose = TRUE
) {

  stopifnot(length(df1_cols_old) == length(df1_cols_new))
  stopifnot(length(df2_cols_old) == length(df2_cols_new))

  # --- Make duplicate names explicit (prevents weird rename/bind_rows behavior) ---
  names(df1) <- make.unique(names(df1))
  names(df2) <- make.unique(names(df2))

  # --- Local helper: inspect columns (replaces your external inspect_columns) ---
  inspect_columns_local <- function(n1, n2) {
    list(
      only_df1 = setdiff(n1, n2),
      only_df2 = setdiff(n2, n1),
      common   = intersect(n1, n2)
    )
  }

  # --- Local helper: rename with mapping; warns if "old" names are missing ---
  rename_with_map <- function(data, old_names, new_names, label = "df") {
    missing <- setdiff(old_names, names(data))
    if (verbose && length(missing) > 0) {
      message(label, ": old names not found (skipped): ",
              paste(missing, collapse = ", "))
    }

    # IMPORTANT: dplyr::rename expects new_name = old_name
    rename_map <- stats::setNames(old_names, new_names)  # new -> old
    dplyr::rename(data, dplyr::any_of(rename_map))
  }

  # --- Rename ---
  df1_renamed <- rename_with_map(df1, df1_cols_old, df1_cols_new, label = "df1")
  df2_renamed <- rename_with_map(df2, df2_cols_old, df2_cols_new, label = "df2")

  # --- Find common columns ---
  common_cols <- intersect(names(df1_renamed), names(df2_renamed))

  # --- Align types for common columns (robust: compare classes, coerce both sides) ---
  type_mismatch_cols <- common_cols[
    vapply(common_cols, function(nm) {
      !identical(class(df1_renamed[[nm]]), class(df2_renamed[[nm]]))
    }, logical(1))
  ]

  if (verbose && length(type_mismatch_cols) > 0) {
    message("Coercing to character due to type mismatch: ",
            paste(type_mismatch_cols, collapse = ", "))
  }

  df1_aligned <- dplyr::mutate(df1_renamed,
                               dplyr::across(dplyr::all_of(type_mismatch_cols), as.character))
  df2_aligned <- dplyr::mutate(df2_renamed,
                               dplyr::across(dplyr::all_of(type_mismatch_cols), as.character))

  venn_after <- inspect_columns_local(names(df1_aligned), names(df2_aligned))

  # --- Bind ---
  df_merged <- dplyr::bind_rows(df1_aligned, df2_aligned)

  list(
    df1_renamed = df1_aligned,
    df2_renamed = df2_aligned,
    venn_after  = venn_after,
    df_merged   = df_merged
  )
}