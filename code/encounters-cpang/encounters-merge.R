# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(readr)
library(arrow)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

# --- Read in Combined Data ---

df1 <- read_feather("data/ice-raw/encounters-selected/f082025_combined.feather")
df2 <- read_feather("data/ice-raw/encounters-selected/uwchr_combined.feather")

# Step 1 (a). Inspect the columns that are shared and unique between datasets
venn_1_2b <- inspect_columns(names(df1), names(df2)) 

# Step 2: Merge columns 
df2_cols_old <- c("Area_of_Responsibility", "Landmark")
df2_cols_new <- c("Responsible_AOR", "Event_Landmark")

merge_1_2 <- merge_dfs(df1, df2, character(0), character(0), df2_cols_old, df2_cols_new)

df12 <- merge_1_2$df_merged
venn_1_2a <- merge_1_2$venn_after

# --- Write out Merged Data ---
write_feather(df12, "data/ice-processed/encounters-merged.feather")
