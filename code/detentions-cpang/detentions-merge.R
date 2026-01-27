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

df1 <- read.csv("data/ice-raw/detentions-selected/2019-ICFO-21307_combined.csv", stringsAsFactors = FALSE)
df2 <- read.csv("data/ice-raw/detentions-selected/2023_ICFO_42034_combined.csv", stringsAsFactors = FALSE)
df3 <- read.csv("data/ice-raw/detentions-selected/2024-ICFO-41855_combined.csv", stringsAsFactors = FALSE)
df4 <- read.csv("data/ice-raw/detentions-selected/120125_combined.csv", stringsAsFactors = FALSE)
df5 <- read.csv("data/ice-raw/detentions-selected/uwchr_combined.csv", stringsAsFactors = FALSE)
df6 <- read.csv("data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF_combined.csv", stringsAsFactors = FALSE)
df7 <- read.csv("data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527_combined.csv", stringsAsFactors = FALSE)

# Step 1 (a). Inspect the columns that are shared and unique between datasets
df_list <- list(
  df1 = df1,
  df2 = df2,
  df3 = df3,
  df4 = df4,
  df5 = df5, 
  df6 = df6, 
  df7 = df7
)
shared_cols_matrix <- get_shared_cols(df_list)
print(shared_cols_matrix)

# df2 and df4 have 23 shared columns
venn_df_2_4 <- inspect_columns(names(df2), names(df4))

# Create "_Date" columns for "Date_Time" columns in df4
colnames(df4)[colnames(df4) == "Book_In_Date_Time"] <- "Detention_Book_In_Date_Time" # Guess based on context
df4$Stay_Book_In_Date <- as.Date(df4$Stay_Book_In_Date_Time)
df4$Detention_Book_In_Date <- as.Date(df4$Detention_Book_In_Date_Time)
df4$Detention_Book_Out_Date <- as.Date(df4$Detention_Book_Out_Date_Time)

# Merge df2 and df4
df2_cols_old <- c("Marital", "Anonymized_Identifier")
df2_cols_new <- c("Marital_Status", "Unique_Identifier")
df_2_4_merged <- merge_dfs(df2, df4, df2_cols_old, df2_cols_new, NULL, NULL)
df24 <- df_2_4_merged$df_merged
venn_df_2_4a <- df_2_4_merged$venn_after

# Merge df5 with df24
venn_df_24_5 <- inspect_columns(names(df24), names(df5))

# Change df5 names and add _Date column 
colnames(df5)[colnames(df5) == "Detention_Book_In_Date_And_Time"] <- "Detention_Book_In_Date_Time"
df5$Detention_Book_In_Date <- as.Date(df5$Detention_Book_In_Date_Time)

df24_cols_old <- c("Most_Serious_Conviction_.MSC._Charge_Code")
df24_cols_new <- c("MSC_Charge_Code")
df5_cols_old <- c("Ethnic", "Anonymized_Identifier")
df5_cols_new <- c("Ethnicity", "Unique_Identifier")
df_24_5_merged <- merge_dfs(df24, df5, df24_cols_old, df24_cols_new, df5_cols_old, df5_cols_new)
df245 <- df_24_5_merged$df_merged
venn_df_24_5a <- df_24_5_merged$venn_after

# Writing this out since it's giving me an memory issue? (which is weird...)
write.csv(df24, "code/detentions-cpang/merge_step1_df24.csv", row.names = FALSE)
