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
source("code/functions/summarize_weekly_counts.R")

df1 <- read_parquet("data/cache/detentions-2019-ICFO-21307.parquet")|> as_tibble()
df2 <- read_parquet("data/cache/detentions-2023_ICFO_42034.parquet")|> as_tibble()
df3 <- read_parquet("data/cache/detentions-2024-ICFO-41855.parquet")|> as_tibble()
df4 <- read_parquet("data/cache/detentions-120125.parquet")|> as_tibble()
df5 <- read_parquet("data/cache/detentions-uwchr.parquet")|> as_tibble()
df6 <- read_parquet("data/cache/detentions-From-Emily-Excel-X-RIF.parquet")|> as_tibble()
df7 <- read_parquet("data/cache/detentions-From-Emily-FOIA-10-2554-527.parquet")|> as_tibble()
df8 <- read_parquet("data/cache/detentions-nov2025.parquet")|> as_tibble()

df1_weekly_counts <- get_weekly_counts(df1, "Book_In_Date")
df2_weekly_counts <- get_weekly_counts(df2, "Detention_Book_In_Date")
df3_weekly_counts <- get_weekly_counts(df3, "Book_in_Date_And_Time")
df4_weekly_counts <- get_weekly_counts(df4, "Book_In_Date_Time")
df5_weekly_counts <- get_weekly_counts(df5, "Detention_Book_In_Date_And_Time")
df6_weekly_counts <- get_weekly_counts(df6, "History_Intake_Date")
df7_weekly_counts <- get_weekly_counts(df7, "Book_In_Date")
df8_weekly_counts <- get_weekly_counts(df8, "Book_In_Date_Time")

all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts, df5_weekly_counts, df6_weekly_counts, df7_weekly_counts, df8_weekly_counts)|>
  arrange(week_start)

ggplot(all_weekly_counts,aes(week_start, y=n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

boundaries <- all_weekly_counts |> 
  group_by(source_file) |> 
  summarise(start = min(week_start), end = max(week_start))

temp <- all_weekly_counts |>
  filter(source_file == "From-Emily-FOIA-10-2554-527"|source_file == "From-Emily-Excel-X-RIF"|source_file == "uwchr"|source_file == "120125")

ggplot(temp ,aes(week_start, y=n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

# at minimum, we merge "120125", "From-Emily-Excel-X-RIF", "From-Emily-FOIA-10-2554-527", and "uwchr"  



