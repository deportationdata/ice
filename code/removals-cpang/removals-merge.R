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

df23_cols_old <- c("MSC_Charge",
                  "MSC_Charge_Date",
                  "MSC_Charge_Code",
                  "MSC_Conviction_Date", 
                  "MSC_Criminal_Charge_Status", 
                  "Latest_Person_Departed_Date")

df23_cols_new <- c("Most_Serious_Criminal_Conviction", 
                  "Most_Serious_Criminal_Conviction_Charge_Date", 
                  "Most_Serious_Criminal_Conviction_Code", 
                  "Most_Serious_Criminal_Conviction_Date",
                  "Most_Serious_Criminal_Conviction_Status", 
                  "Most_Recent_Prior_Depart_Date")

df1_cols_old <- c("Port_Of_Departure", 
                "Departed_To_Country", 
                "Country_of_Birth", 
                "Country_of_Citizenship", 
                "Year_of_Birth", 
                "Rc_Threat_Level", 
                "Unique_ID")

df1_cols_new <- c("Port_of_Departure", 
                "Departure_Country", 
                "Birth_Country", 
                "Citizenship_Country", 
                "Birth_Year", 
                "Case_Threat_Level", 
                "Unique_Identifier")

merge_23_1 <- merge_dfs(df23, df1, df23_cols_old, df23_cols_new, df1_cols_old, df1_cols_new)
df231 <- merge_23_1$df_merged
venn_23_1_after <- merge_23_1$venn_after

venn_231_4 <- inspect_columns(names(df231), names(df4))
df2314 <- bind_rows(df231, df4)

# --- Write out file ---
write.csv(df2314, "data/ice-processed/removals-merged.csv")
