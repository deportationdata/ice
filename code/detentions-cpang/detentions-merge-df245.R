# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(data.table)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

# --- Read in Combined Data ---
df24 <- fread("code/detentions-cpang/merge_step1_df24.csv", showProgress = TRUE)
df5 <- fread("data/ice-raw/detentions-selected/uwchr_combined.csv", showProgress = TRUE)

# Merge df5 with df24
venn_df_24_5 <- inspect_columns(names(df24), names(df5))

# Change df5 names and add _Date column 
colnames(df5)[colnames(df5) == "Detention_Book_In_Date_And_Time"] <- "Detention_Book_In_Date_Time"
df5$Detention_Book_In_Date <- as.Date(df5$Detention_Book_In_Date_Time)

df24_cols_old <- c("Most_Serious_Conviction_.MSC._Charge_Code")
df24_cols_new <- c("MSC_Charge_Code")
df5_cols_old <- c("Ethnic", "Anonymized_Identifier")
df5_cols_new <- c("Ethnicity", "Unique_Identifier")
df_24_5_merged <- merge_dfs_to_parquet_dataset(df24, df5, df24_cols_old, df24_cols_new, df5_cols_old, df5_cols_new, out_dir = "code/detentions-cpang/merged_step2_parquet")
