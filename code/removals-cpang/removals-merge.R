# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

# --- Read in Combined Data ---

df1 <- read.csv("data/ice-raw/removals-selected/14-03290_combined.csv", stringsAsFactors = FALSE)
df2 <- read.csv("data/ice-raw/removals-selected/2023_ICFO_42034_combined.csv", stringsAsFactors = FALSE)
df3 <- read.csv("data/ice-raw/removals-selected/082025_combined.csv", stringsAsFactors = FALSE)
df4 <- read.csv("data/ice-raw/removals-selected/uwchr_combined.csv", stringsAsFactors = FALSE)

# Step 0. Get Shared column matrix to determine which datasets to compare FIRST 
df_list <- list(df1 = df1, 
                df2 = df2, 
                df3 = df3, 
                df4 = df4)
shared_col_matrix <- get_shared_cols(df_list)
print(shared_col_matrix)

# Step 1 (a). Inspect the columns that are shared and unique between datasets
venn_1_2b <- inspect_columns(names(df2), names(df3)) 
# --- There appears to be 13 overlapping columns between df1 and df2 

# Step 1 (b). Flag near-matches between datasets
near_matches_1_2 <- flag_near_matches(names(df2), names(df1), max_dist = 2)

df2_cols_old <- c("Anonymized_Identifer", "Departure_Date")
df2_cols_new <- c("Unique_Identifier", "Departed_Date")

merge_2_3 <- merge_dfs(df2, df3, df2_cols_old, df2_cols_new, NULL, NULL)
df23 <- merge_2_3$df_merged
venn_2_3a <- merge_2_3$venn_after

# --- Merge df23 and df1 --- 
venn_23_1 <- inspect_columns(names(df23), names(df1))
