library(dplyr)
library(purrr)
library(tibble)


# Assumes inspect_columns() is available from your sourced file
source("code/functions/inspect_columns.R")

merge_dfs_old <- function(df1, df2, df1_cols_old, df1_cols_new, df2_cols_old, df2_cols_new){ 
# Rename columns in df1 ]
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
  } # Coerce common columns to the same data type 
  common_cols <- intersect(names(df1), names(df2)) 
  for (col in common_cols) { 
    if (!identical(class(df1[[col]]), class(df2[[col]]))) { 
      df1[[col]] <- as.character(df1[[col]]) 
      df2[[col]] <- as.character(df2[[col]]) 
      } 
  } # See which columns are now shared 
  venn_after <- inspect_columns(names(df1), names(df2)) # merged data frames with renamed columns 
  df_merged <- bind_rows(df1, df2) 
  return(list(df1_renamed = df1, df2_renamed = df2, venn_after = venn_after, df_merged = df_merged)) 
}