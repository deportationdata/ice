library(dplyr)
library(purrr)
library(tibble)

# Assumes inspect_columns() is available from your sourced file
source("code/functions/inspect_columns.R")

merge_dfs <- function(
  df1, df2,
  df1_cols_old, df1_cols_new,
  df2_cols_old, df2_cols_new
) {

  stopifnot(length(df1_cols_old) == length(df1_cols_new))
  stopifnot(length(df2_cols_old) == length(df2_cols_new))

  rename_with_map <- function(data, old_names, new_names) {
    rename_map <- set_names(new_names, old_names)
    data %>% rename(any_of(rename_map))
  }

  df1_renamed <- df1 %>% rename_with_map(df1_cols_old, df1_cols_new)
  df2_renamed <- df2 %>% rename_with_map(df2_cols_old, df2_cols_new)

  common_cols <- intersect(names(df1_renamed), names(df2_renamed))

  type_mismatch_cols <- common_cols %>%
    keep(~ !identical(class(df1_renamed[[.x]]), class(df2_renamed[[.x]])))

  df1_aligned <- df1_renamed %>%
    mutate(across(all_of(type_mismatch_cols), as.character))

  df2_aligned <- df2_renamed %>%
    mutate(across(all_of(type_mismatch_cols), as.character))

  venn_after <- inspect_columns(names(df1_aligned), names(df2_aligned))

  df_merged <- bind_rows(df1_aligned, df2_aligned)

  list(
    df1_renamed = df1_aligned,
    df2_renamed = df2_aligned,
    venn_after = venn_after,
    df_merged = df_merged
  )
}
