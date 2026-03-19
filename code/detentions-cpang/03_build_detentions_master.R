# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(data.table)
library(arrow)
library(ggplot2)
library(lubridate)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")
source("code/functions/summarize_weekly_counts.R")

df4 <- read_feather("data/ice-raw/detentions-selected/120125_combined.feather")|> as_tibble()
df5 <- read_feather("data/ice-raw/detentions-selected/uwchr_combined.feather")|> as_tibble()
df6 <- read_feather("data/ice-raw/detentions-selected/From-Emily-Excel-X-RIF_combined.feather")|> as_tibble()
df7 <- read_feather("data/ice-raw/detentions-selected/From-Emily-FOIA-10-2554-527_combined.feather")|> as_tibble()

# filter dates that we want 
df6_trimmed <- df6 |>
  filter(History_Intake_Date > as.Date("2010-05-23")+7 & History_Intake_Date < as.Date("2014-10-12"))
df5_trimmed <- df5 |>
  filter(Detention_Book_In_Date_And_Time >= as.Date("2014-10-12") & Detention_Book_In_Date_And_Time < as.Date("2023-12-31"))
df4_trimmed <- df4 |> 
  filter(Book_In_Date_Time >= as.Date("2023-12-31"))

# merge dataframes! 
venn_6_7 <- inspect_columns(names(df6_trimmed), names(df7))
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

merge_6_7 <- merge_dfs(df6_trimmed, df7, df6_old_cols, df6_new_cols, df7_old_cols, df7_new_cols)
df67 <- merge_6_7$df_merged

venn_4_5<- inspect_columns(names(df4_trimmed), names(df5_trimmed))
df4_cols_old <- c("Book_In_Date_Time", "Most_Serious_Conviction_MSC_Charge_Code")
df4_cols_new <- c("Detention_Book_In_Date_And_Time", "Most_Serious_Conviction_Charge_Code")
df5_cols_old <- c("Ethnic", "MSC_Charge_Code", "Anonymized_Identifier")
df5_cols_new <- c("Ethnicity", "Most_Serious_Conviction_Charge_Code", "Unique_Identifier")
merge_4_5 <- merge_dfs(df4_trimmed, df5_trimmed, df4_cols_old, df4_cols_new, df5_cols_old, df5_cols_new)
df45 <- merge_4_5$df_merged

df45 <- df45 |>
  mutate(Detention_Book_In_Date = as.Date(Detention_Book_In_Date_And_Time), 
          Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date_Time), 
          Birth_Country = coalesce(Birth_Country_ERO, Birth_Country_PER))|>
  select(-Birth_Country_ERO,-Birth_Country_PER)

venn_67_45 <- inspect_columns(names(df67), names(df45))

df67_old_cols <- c("Release_Reason")
df67_new_cols <- c("Detention_Release_Reason")
df45_old_cols <- c("Docket_Control_Office")
df45_new_cols <- c("Book_In_DCO")

rm(list = setdiff(ls(), c("df67", "df45", "df67_old_cols", "df67_new_cols", "df45_old_cols", "df45_new_cols")))
gc()

merge_all <- merge_dfs(df67, df45, df67_old_cols, df67_new_cols, df45_old_cols, df45_new_cols)
df_all <- merge_all$df_merged

# check final dataframe
all_weekly_counts <- get_weekly_counts(df_all, "Detention_Book_In_Date")

ggplot(all_weekly_counts ,aes(week_start, y=n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

write_feather(df_all, "data/ice-final/detentions-final-v3.feather")
