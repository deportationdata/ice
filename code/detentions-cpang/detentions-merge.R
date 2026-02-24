# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(data.table)
library(arrow)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

# --- Read in Combined Data ---
df1 <- read_feather("data/ice-raw/detentions-selected/2019-ICFO-21307_combined.feather")|> as_tibble()
df2 <- read_feather("data/ice-raw/detentions-selected/2023_ICFO_42034_combined.feather")|> as_tibble()
df3 <- read_feather("data/ice-raw/detentions-selected/2024-ICFO-41855_combined.feather")|> as_tibble()
df4 <- read_feather("data/ice-raw/detentions-selected/120125_combined.feather")|> as_tibble()
df5 <- read_feather("data/ice-raw/detentions-selected/uwchr_combined.feather")|> as_tibble()
df6 <- read_feather("data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF_combined.feather")|> as_tibble()
df7 <- read_feather("data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527_combined.feather")|> as_tibble()

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
df4 <- df4 %>%
  rename(
    Detention_Book_In_Date_Time = Book_In_Date_Time
  ) %>%
  mutate(
    Stay_Book_In_Date        = as.Date(Stay_Book_In_Date_Time),
    Detention_Book_In_Date   = as.Date(Detention_Book_In_Date_Time),
    Detention_Book_Out_Date  = as.Date(Detention_Book_Out_Date_Time)
  )

# Merge df2 and df4
df2_cols_old <- c("Marital", "Anonymized_Identifier", "Case_ID", "Subject_ID")
df2_cols_new <- c("Marital_Status", "Unique_Identifier", "EID_Case_ID", "EID_Subject_ID")
df_2_4_merged <- merge_dfs(df2, df4, df2_cols_old, df2_cols_new, character(0), character(0))
df24 <- df_2_4_merged$df_merged
venn_df_2_4a <- df_2_4_merged$venn_after

## drop variables relating to df2 and df4 to free up memory 
rm(df2, df4, df_2_4_merged)
rm(df2_cols_new, df2_cols_old)
gc()

# Merge df5 with df24
venn_df_24_5 <- inspect_columns(names(df24), names(df5))

# Change df5 names and add _Date column 
df5 <- df5 %>%
  rename(Detention_Book_In_Date_Time = Detention_Book_In_Date_And_Time) %>%
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date_Time)
  )

df24_cols_old <- c("Most_Serious_Conviction_MSC_Charge_Code")
df24_cols_new <- c("MSC_Charge_Code")
df5_cols_old <- c("Ethnic", "Anonymized_Identifier")
df5_cols_new <- c("Ethnicity", "Unique_Identifier")
df_24_5_merged <- merge_dfs(df24, df5, df24_cols_old, df24_cols_new, df5_cols_old, df5_cols_new)
df245 <- df_24_5_merged$df_merged
venn_df_24_5a <- df_24_5_merged$venn_after

rm(df24, df5, df_24_5_merged)
rm(df24_cols_old, df24_cols_new, df5_cols_new, df5_cols_old)
gc()

# Merge df245 and df1 (hopefully this works but we'll see)
venn_df_245_1 <- inspect_columns(names(df245), names(df1))
df245_cols_old <- c("Area_of_Responsibility", "Docket_Control_Office")
df245_cols_new <- c("Book_In_AOR", "Book_In_DCO")

df1_cols_old <- c("Detention_Id", "Book_In_Date", "Book_In_Date_And_Time",
                  "Book_Out_Date_And_Time", "Book_Out_Date",
                  "Book_In_Aor", "Book_In_Dco", "Release_Reason", "Eid_Cse_Id")
df1_cols_new <- c(
  "Detention_ID",
  "Detention_Book_In_Date",
  "Detention_Book_In_Date_Time",
  "Detention_Book_Out_Date_Time",
  "Detention_Book_Out_Date",
  "Book_In_AOR",
  "Book_In_DCO", 
  "Detention_Release_Reason", 
  "EID_Case_ID"
)

merge_245_1 <- merge_dfs(df245, df1, df245_cols_old, df245_cols_new, df1_cols_old, df1_cols_new)
df2451 <- merge_245_1$df_merged
venn_df_245_1a <- merge_245_1$venn_after

rm(df245, df1, merge_245_1)
rm(df1_cols_old, df1_cols_new, df245_cols_old, df245_cols_new)
gc()



# df's left: 3, 6, 7 
# naive merge for 6 and 7 first, since those df's are "lighter"
venn_6_7 <- inspect_columns(names(df6), names(df7))

df6_old_cols <- c(
  "History_Detention_Facility",
  "History_Detention_Facility_Code",
  "History_Intake_Date",
  "History_Book_out_Date",
  "History_Release_Reason",
  "ERO_Apprehension_Date",
  "ERO_Apprehension_Landmark",
  "Initial_Intake_Detention_Facility",
  "Order_of_Detentions", 
  "History_Intake_DCO"
)

df6_new_cols <- c(
  "Detention_Facility",
  "Detention_Facility_Code",
  "Detention_Book_In_Date",
  "Detention_Book_Out_Date",
  "Release_Reason",
  "Apprehension_Date",
  "Apprehension_Landmark",
  "Initial_Book_In_Facility",
  "Facility_Held_In_Seq", 
  "Book_In_DCO"
)

df7 <- df7 %>%
  rename_with(~ gsub("\\s+", "_", .x))

df7_old_cols <- c("Book_In_Date", "Book_Out_Date", "Book_In_Dco")
df7_new_cols <- c("Detention_Book_In_Date", "Detention_Book_Out_Date", "Book_In_DCO")

merge_6_7 <- merge_dfs(df6, df7, df6_old_cols, df6_new_cols, df7_old_cols, df7_new_cols)
df67 <- merge_6_7$df_merged
venn_6_7a <- merge_6_7$venn_after

rm(df6, df7, merge_6_7)
rm(df6_new_cols, df6_old_cols, df7_new_cols, df7_old_cols)
gc()

# Merge df67 with df3
venn_67_3 <- inspect_columns(names(df67), names(df3))

df67_old_cols <- c("City", "State")
df67_new_cols <- c("Detention_Facility_City", "Detention_Facility_State")

df3_old_cols <- c("Book_in_DCO")
df3_new_cols <- c("Book_In_DCO")

# add new columns to Book_In_Date/Time objects in df3
df3 <- df3 %>%
  mutate(
    Detention_Book_In_Date  = substr(Book_in_Date_And_Time, 1, 10),
    Detention_Book_Out_Date = substr(Book_Out_Date_Time, 1, 10),
    Initial_Book_In_Date    = substr(Initial_Book_In_Date_Time, 1, 10)
  )

merge_67_3 <- merge_dfs(df67, df3, df67_old_cols, df67_new_cols, df3_old_cols, df3_new_cols)
venn_67_3a <- merge_67_3$venn_after
df673 <- merge_67_3$df_merged

rm(df67, df3, merge_67_3)
rm(df3_new_cols, df3_old_cols, df67_new_cols, df67_old_cols)
gc()

# Master merge: df2451 and df673
venn_df_2451_673 <- inspect_columns(names(df2451), names(df673))

df673_old_cols <- c("Book_Out_Date_Time", "Most_Serious_Conviction_Date", "Most_Serious_Sentence_Months", "Most_Serious_Sentence_Years", "Release_Reason", "Alien_Number_Unique_Identifier", "Book_in_Date_And_Time")
df673_new_cols <- c("Detention_Book_Out_Date_Time", "MSC_Conviction_Date", "MSC_Sentence_Months", "MSC_Sentence_Years", "Detention_Release_Reason", "Unique_Identifier", "Detention_Book_In_Date_Time")

df2451_old_cols <- c("Eid_Dt_Civ_Id")
df2451_new_cols <- c("EID_Civilian_Id")

merge_all <- merge_dfs(df2451, df673,  df2451_old_cols, df2451_new_cols, df673_old_cols, df673_new_cols)
venn_all <- merge_all$venn_after

df_all <- merge_all$df_merged

rm(df2451, df673, merge_all)
gc()


## Write out!!!! 
write_feather(df_all, "data/ice-processed/detentions-merged.feather")
