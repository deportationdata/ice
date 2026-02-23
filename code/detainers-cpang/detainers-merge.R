# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(arrow)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

# --- Read in Combined Data ---

df1 <- read_feather("data/ice-raw/detainers-selected/2025-ICFO-18038_combined.feather")
df2 <- read_feather("data/ice-raw/detainers-selected/120125_combined.feather")
df3 <- read_feather("data/ice-raw/detainers-selected/npr_combined.feather")

# Merge Detention_Facility and Detainer_Detention_Facility in df3 
df3 <- df3 %>%
  mutate(
    Detention_Facility = coalesce(
      Detention_Facility,
      Detainer_Detention_Facility
    )
  )

# Step 0. Get Shared column matrix to determine which datasets to compare FIRST 
df_list <- list(df1 = df1, 
                df2 = df2, 
                df3 = df3)
shared_col_matrix <- get_shared_cols(df_list)
print(shared_col_matrix)


# Step 1 (a). Inspect the columns that are shared and unique between datasets
venn_1_2b <- inspect_columns(names(df1), names(df2)) 
# --- There appears to be 13 overlapping columns between df1 and df2 

# Step 1 (b). Flag near-matches between datasets
near_matches_1_2 <- flag_near_matches(names(df2), names(df1), max_dist = 2)

# Step 2. Rename the columns that are probably mergeable (i.e., similar names for the same field)
df1_cols_old <- c("Multiple_Prior_Misd_Yes_No",
                  "Program", 
                  "Detainer_Most_Serious_Conviction_Charge_Code", 
                  "Detainer_Most_Serious_Conviction_Charge",
                  "Detainer_Most_Serious_Conviction_Conviction_Date",
                  "Detainer_Most_Serious_Conviction_Sentence_Days",
                  "Detainer_Most_Serious_Conviction_Sentence_Months",
                  "Detainer_Most_Serious_Conviction_Sentence_Years",
                  "Alien_Number_Unique_Identifier", 
                  "Detainer_Most_Serious_Conviction_Charge_Date", 
                  "Detainer_Detention_Facility", 
                  "Detainer_Detention_Facility_Code")
df1_cols_new <- c("Multiple_Prior_MISD_Yes_No",
                  "Final_Program",
                  "Most_Serious_Conviction_Charge_Code",
                  "Most_Serious_Conviction_Charge",
                  "Most_Serious_Conviction_Conviction_Date",
                  "Most_Serious_Conviction_Sentence_Days",
                  "Most_Serious_Conviction_Sentence_Months",
                  "Most_Serious_Conviction_Sentence_Years",
                  "Unique_Identifier", 
                  "Most_Serious_Conviction_Charge_Date",
                  "Detention_Facility",
                  "Detention_Facility_Code")

df2_cols_old <- c("Detainer_Prep_Threat_Level",
                  "MSC_Charge_Code",
                  "Most_Serious_Conviction_MSC_Charge",
                  "MSC_Conviction_Date",
                  "MSC_Sentence_Days",
                  "MSC_Sentence_Months",
                  "MSC_Sentence_Years", 
                  "MSC_Charge_Date"
)

df2_cols_new <- c("Detainer_Threat_Level",
                  "Most_Serious_Conviction_Charge_Code",
                  "Most_Serious_Conviction_Charge",
                  "Most_Serious_Conviction_Conviction_Date",
                  "Most_Serious_Conviction_Sentence_Days",
                  "Most_Serious_Conviction_Sentence_Months",
                  "Most_Serious_Conviction_Sentence_Years", 
                  "Most_Serious_Conviction_Charge_Date"
)

merge_1_2 <- merge_dfs(df1, df2,
                         df1_cols_old, df1_cols_new,
                         df2_cols_old, df2_cols_new)

df12 <- merge_1_2$df_merged
venn_1_2_after <- merge_1_2$venn_after

# Merge df12 with df3
venn_12_3b <- inspect_columns(names(df12), names(df3))

df12_cols_old <- c("Detainer_Prepare_Date")
df12_cols_new <- c("Prepare_Date")

df3_cols_old <- c("Detainer_Lift_Reason_2_Code",
                  "Osc_Served_Yes_No",
                  "Osc_Served_Date",
                "Area_Of_Responsibility")

df3_cols_new <- c("Detainer_Lift_Reason_Code2",
                  "Order_to_Show_Cause_Served_Yes_No",
                  "Order_to_Show_Cause_Served_Date",
                "Detainer_AOR")
merge_12_3 <- merge_dfs(df12, df3,
                          df12_cols_old, df12_cols_new,
                          df3_cols_old, df3_cols_new)

df123 <- merge_12_3$df_merged
venn_12_3_after <- merge_12_3$venn_after

# drop Detainer_Detention_Facility 
df_final<- df123 %>%
  select(-Detainer_Detention_Facility)

# --- Write out merged data ---
write_feather(df_final, "data/ice-processed/detainers-merged.feather")
