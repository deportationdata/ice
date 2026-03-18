# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(data.table)
library(arrow)
library(ggplot2)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

df3 <- read_feather("data/ice-raw/detentions-selected/2024-ICFO-41855_combined.feather")|> as_tibble()
df4 <- read_feather("data/ice-raw/detentions-selected/120125_combined.feather")|> as_tibble()
df5 <- read_feather("data/ice-raw/detentions-selected/uwchr_combined.feather")|> as_tibble()
df6 <- read_feather("data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF_combined.feather")|> as_tibble()
df7 <- read_feather("data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527_combined.feather")|> as_tibble()

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
df7_old_cols <- c("Book_In_Date", "Book_Out_Date", "Book_In_Dco")
df7_new_cols <- c("Detention_Book_In_Date", "Detention_Book_Out_Date", "Book_In_DCO")

merge_6_7 <- merge_dfs(df6, df7, df6_old_cols, df6_new_cols, df7_old_cols, df7_new_cols)
df67 <- merge_6_7$df_merged
rm(df6, df7, merge_6_7)

venn_67_3 <- inspect_columns(names(df67), names(df3))

df67_old_cols <- c("City", "State")
df67_new_cols <- c("Detention_Facility_City", "Detention_Facility_State")

df3_old_cols <- c("Book_in_DCO")
df3_new_cols <- c("Book_In_DCO")

# add new columns to Book_In_Date/Time objects in df3
df3 <- df3 |>
  mutate(
    Detention_Book_In_Date  = as.Date(Book_in_Date_And_Time),
    Detention_Book_Out_Date = as.Date(Book_Out_Date_Time),
    Initial_Book_In_Date    = as.Date(Initial_Book_In_Date_Time)
  )

merge_67_3 <- merge_dfs(df67, df3, df67_old_cols, df67_new_cols, df3_old_cols, df3_new_cols)
df673 <- merge_67_3$df_merged
rm(venn_6_7, venn_67_3, df3, merge_67_3, df67)
gc()

venn_4_5<- inspect_columns(names(df4), names(df5))
df4_cols_old <- c("Book_In_Date_Time", "Most_Serious_Conviction_MSC_Charge_Code")
df4_cols_new <- c("Detention_Book_In_Date_And_Time", "Most_Serious_Conviction_Charge_Code")
df5_cols_old <- c("Ethnic", "MSC_Charge_Code", "Anonymized_Identifier")
df5_cols_new <- c("Ethnicity", "Most_Serious_Conviction_Charge_Code", "Unique_Identifier")
merge_4_5 <- merge_dfs(df4, df5, df4_cols_old, df4_cols_new, df5_cols_old, df5_cols_new)
df45 <- merge_4_5$df_merged
rm(venn_4_5, df4_cols_old, df4_cols_new, df5_cols_old, df5_cols_new, merge_4_5, df4, df5)

df45 <- df45 |>
  mutate(Detention_Book_In_Date = as.Date(Detention_Book_In_Date_And_Time), 
          Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date_Time), 
          Birth_Country = coalesce(Birth_Country_ERO, Birth_Country_PER))
venn_673_45 <- inspect_columns(names(df673), names(df45))

df673_old_cols <- c("Release_Reason", "Book_in_Criminality", "Book_in_Date_And_Time", "Book_Out_Date_Time", "Alien_Number_Unique_Identifier")
df673_new_cols <- c("Detention_Release_Reason", "Book_In_Criminality", "Detention_Book_In_Date_Time", "Detention_Book_Out_Date_Time", "Unique_Identifier")
df45_old_cols <- c("Detention_Book_In_Date_And_Time", "Docket_Control_Office", "Area_of_Responsibility", "MSC_Charge", "Most_Serious_Conviction_MSC_Criminal_Charge_Category", "MSC_Conviction_Date", "MSC_Sentence_Days", "MSC_Sentence_Months", "MSC_Sentence_Years", "MSC_Crime_Class")
df45_new_cols <- c("Detention_Book_In_Date_Time", "Book_In_DCO", "Apprehension_AOR", "Most_Serious_Conviction_Charge", "Most_Serious_Conviction_Criminal_Charge_Category", "Most_Serious_Conviction_Conviction_Date", "Most_Serious_Conviction_Sentence_Days", "Most_Serious_Conviction_Sentence_Months", "Most_Serious_Conviction_Sentence_Years", "Most_Serious_Conviction_Crime_Class")

merge_all <- merge_dfs(df673, df45, df673_old_cols, df673_new_cols, df45_old_cols, df45_new_cols)
