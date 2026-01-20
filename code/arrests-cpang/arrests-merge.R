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
df1 <- read.csv("data/ice-raw/arrests-selected/2022-ICFO-22955_combined.csv", stringsAsFactors = FALSE)
df2 <- read.csv("data/ice-raw/arrests-selected/2023_ICFO_42034_combined.csv", stringsAsFactors = FALSE)
df3 <- read.csv("data/ice-raw/arrests-selected/120125_combined.csv", stringsAsFactors = FALSE)
df4 <- read.csv("data/ice-raw/arrests-selected/uwchr_combined.csv", stringsAsFactors = FALSE)

# Step 0. Get Shared column matrix to determine which datasets to compare FIRST 
df_list <- list(df1 = df1, 
                df2 = df2, 
                df3 = df3, 
                df4 = df4)
shared_col_matrix <- get_shared_cols(df_list)
print(shared_col_matrix)

# Step 1 (a). Inspect the columns that are shared and unique between datasets
venn_1_2b <- inspect_columns(names(df1), names(df3)) 
# --- There appears to be 13 overlapping columns between df1 and df2 

# Step 1 (b). Flag near-matches between datasets
near_matches_1_2 <- flag_near_matches(names(df3), names(df1), max_dist = 2)

# Step 2. Rename the columns that are probably mergeable (i.e., similar names for the same field)
df1$Apprehension_Date <- df1$Apprehension_Date <- substr(df1$Apprehension_Date_And_Time, 1, 10)

df1_cols_old <- c("Final_Order_Yes_No.Blank",
                      "Sequence_Number.Unique_Identifier",
                      "County",
                      "State",
                      "Apprehension_Final_Program",
                      "ApprehensionFinal_Program_Group",
                      "Numeric_Birth_Year")
df1_cols_new <- c("Final_Order_Yes_No",
                      "Unique_Identifier",
                      "Apprehension_County",
                      "Apprehension_State",
                      "Final_Program",
                      "Final_Program_Group",
                      "Birth_Year")
df3_cols_old <- c("Apprehension_Site_Landmark")
df3_cols_new <- c("Apprehension_Landmark")

# Step 3. Merge the datasets
merge_1_3_out <- merge_dfs(df1, df3,
                        df1_cols_old, df1_cols_new,
                        df3_cols_old, df3_cols_new)
df13 <- merge_1_3_out$df_merged

# merge df13 with df2
venn_13_2b <- inspect_columns(names(df13), names(df2))
df2_cols_old <- c("Anonymized_Identifier")
df2_cols_new <- c("Unique_Identifier")
merge_13_2_out <- merge_dfs(df13, df2,
                            NULL, NULL,
                            df2_cols_old, df2_cols_new)

## ! NOTE: Consider in df2 there are columns "Arrest_Created_By", "Arrest_Create_By", and "Arrested_Created_By" that may be the same field
## ! but with different spellings. These columns were not merged in this script.

df132 <- merge_13_2_out$df_merged

# merge df132 with df4
venn_132_4b <- inspect_columns(names(df132), names(df4))
