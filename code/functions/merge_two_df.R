library(arrow)
source("code/functions/inspect_columns.R")

merge_dfs <- function(df1, df2, df1_cols_old, df1_cols_new, df2_cols_old, df2_cols_new){

  # create copies of df1 and df2 to avoid modifying original data frames
  df1_copy <- df1
  df2_copy <- df2

  # Rename columns in df1
  for (i in 1:length(df1_cols_old)) {
    old_name <- df1_cols_old[i]
    new_name <- df1_cols_new[i]
    names(df1_copy)[names(df1_copy) == old_name] <- new_name
  }

  # Rename columns in df2
  for (i in 1:length(df2_cols_old)) {
    old_name <- df2_cols_old[i]
    new_name <- df2_cols_new[i]
    names(df2_copy)[names(df2_copy) == old_name] <- new_name
  }

  # Coerce common columns to the same data type
  common_cols <- intersect(names(df1_copy), names(df2_copy))
  for (col in common_cols) {
    if (!identical(class(df1_copy[[col]]), class(df2_copy[[col]]))) {
      df1_copy[[col]] <- as.character(df1_copy[[col]])
      df2_copy[[col]] <- as.character(df2_copy[[col]])
    }
  }

  # See which columns are now shared
  venn_after <- inspect_columns(names(df1_copy), names(df2_copy))

  # merged data frames with renamed columns
  df_merged <- bind_rows(df1_copy, df2_copy)

  return(list(df1_renamed = df1_copy,
              df2_renamed = df2_copy,
              venn_after = venn_after,
              df_merged = df_merged))
}

merge_dfs_to_parquet_dataset <- function(df1, df2,
                                        df1_cols_old, df1_cols_new,
                                        df2_cols_old, df2_cols_new,
                                        out_dir) {
  stopifnot(requireNamespace("data.table", quietly = TRUE))
  stopifnot(requireNamespace("arrow", quietly = TRUE))

  dt1 <- data.table::as.data.table(df1)
  dt2 <- data.table::as.data.table(df2)

  if (length(df1_cols_old)) data.table::setnames(dt1, df1_cols_old, df1_cols_new, skip_absent = TRUE)
  if (length(df2_cols_old)) data.table::setnames(dt2, df2_cols_old, df2_cols_new, skip_absent = TRUE)

  all_cols <- union(names(dt1), names(dt2))
  for (nm in setdiff(all_cols, names(dt1))) data.table::set(dt1, j = nm, value = NA)
  for (nm in setdiff(all_cols, names(dt2))) data.table::set(dt2, j = nm, value = NA)
  data.table::setcolorder(dt1, all_cols)
  data.table::setcolorder(dt2, all_cols)

  arrow::write_dataset(dt1, out_dir, format = "parquet", existing_data_behavior = "overwrite_or_ignore")
  rm(dt1); gc()
  arrow::write_dataset(dt2, out_dir, format = "parquet", existing_data_behavior = "overwrite_or_ignore")
  rm(dt2); gc()

  invisible(out_dir)
}
