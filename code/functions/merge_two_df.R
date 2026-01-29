library(arrow)
source("code/functions/inspect_columns.R")

merge_dfs <- function(df1, df2, df1_cols_old, df1_cols_new, df2_cols_old, df2_cols_new){

  # Rename columns in df1
  for (i in 1:length(df1_cols_old)) {
    old_name <- df1_cols_old[i]
    new_name <- df1_cols_new[i]
    names(df1)[names(df1) == old_name] <- new_name
  }

  # Rename columns in df2
  for (i in 1:length(df2_cols_old)) {
    old_name <- df2_cols_old[i]
    new_name <- df2_cols_new[i]
    names(df2)[names(df2) == old_name] <- new_name
  }

  # Coerce common columns to the same data type
  common_cols <- intersect(names(df1), names(df2))
  for (col in common_cols) {
    if (!identical(class(df1[[col]]), class(df2[[col]]))) {
      df1[[col]] <- as.character(df1[[col]])
      df2[[col]] <- as.character(df2[[col]])
    }
  }

  # See which columns are now shared
  venn_after <- inspect_columns(names(df1), names(df2))

  # merged data frames with renamed columns
  df_merged <- bind_rows(df1, df2)

  return(list(df1_renamed = df1,
              df2_renamed = df2,
              venn_after = venn_after,
              df_merged = df_merged))
}

merge_dfs_to_parquet <- function(
  df1, df2,
  df1_cols_old, df1_cols_new,
  df2_cols_old, df2_cols_new,
  out_dir = "merged_dataset",
  # type handling controls
  type_strategy = c("vctrs_then_character", "character_for_mismatch", "none"),
  verbose = TRUE
) {
  type_strategy <- match.arg(type_strategy)

  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  rename_fast <- function(df, old, new) {
    stopifnot(length(old) == length(new))
    map <- stats::setNames(new, old)
    idx <- match(names(df), names(map))
    names(df)[!is.na(idx)] <- unname(map[idx[!is.na(idx)]])
    df
  }

  df1r <- rename_fast(df1, df1_cols_old, df1_cols_new)
  df2r <- rename_fast(df2, df2_cols_old, df2_cols_new)

  # ---- Common columns after renaming ----
  common_cols <- intersect(names(df1r), names(df2r))

  # ---- Coerce common columns to compatible types ----
  if (length(common_cols) > 0 && type_strategy != "none") {

    # helper to test "same enough"
    same_type <- function(x, y) {
      identical(typeof(x), typeof(y)) && identical(class(x), class(y))
    }

    for (col in common_cols) {
      x <- df1r[[col]]
      y <- df2r[[col]]
      if (same_type(x, y)) next

      if (type_strategy == "character_for_mismatch") {
        df1r[[col]] <- as.character(x)
        df2r[[col]] <- as.character(y)
        next
      }

      # vctrs_then_character
      ok <- TRUE
      proto <- tryCatch(
        vctrs::vec_ptype2(x, y),
        error = function(e) { ok <<- FALSE; NULL }
      )

      if (ok) {
        df1r[[col]] <- vctrs::vec_cast(x, proto)
        df2r[[col]] <- vctrs::vec_cast(y, proto)
      } else {
        # last resort fallback
        df1r[[col]] <- as.character(x)
        df2r[[col]] <- as.character(y)
      }
    }
  }

  # Write parts (no giant in-memory bind)
  f1 <- file.path(out_dir, "part1.parquet")
  f2 <- file.path(out_dir, "part2.parquet")

  arrow::write_parquet(df1r, f1)
  arrow::write_parquet(df2r, f2)

  if (verbose) {
    message("Wrote: ", f1)
    message("Wrote: ", f2)
    message("Common columns checked: ", length(common_cols))
    message("Open later with: arrow::open_dataset('", out_dir, "')")
  }

  invisible(list(
    out_dir = out_dir,
    part1 = f1,
    part2 = f2,
    common_cols = common_cols
  ))
}
