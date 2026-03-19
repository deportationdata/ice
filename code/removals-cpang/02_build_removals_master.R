# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(stringr)
library(arrow)
library(ggplot2)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/summarize_weekly_counts.R")

df1 <- read_feather("data/ice-raw/removals-selected/2023_ICFO_42034_combined.feather")
df2 <- read_feather("data/ice-raw/removals-selected/082025_combined.feather")
df3 <- read_feather("data/ice-raw/removals-selected/uwchr_combined.feather")
df4 <- read_feather("data/ice-raw/removals-selected/14-03290_combined.feather")

df1_weekly_counts <- get_weekly_counts(df1, "Departure_Date")
df2_weekly_counts <- get_weekly_counts(df2, "Departed_Date")
df3_weekly_counts <- get_weekly_counts(df3, "Departed_Date")
df4_weekly_counts <- get_weekly_counts(df4, "Departed_Date")

all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts)

ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

boundaries <- all_weekly_counts |> 
  group_by(source_file) |> 
  summarise(start = min(week_start), end = max(week_start))

# trim datasets by date and merge (14-03290, 2024_ICFO_42034, 082025)
df4_trimmed <- df4 |>
  filter(Departed_Date < as.Date("2013-09-29"))
df1_trimmed <- df1 |>
  filter(Departure_Date >= as.Date("2013-09-29") & Departure_Date < as.Date("2023-09-24"))
df2_trimmed <- df2 |>
  filter(Departed_Date >= as.Date("2023-09-24"))

# MERGE DFS
venn_1_2b <- inspect_columns(names(df1_trimmed), names(df2_trimmed)) 

# Step 2: Merge columns 
df1_cols_old <- c("Departure_Date", "Anonymized_Identifier")
df1_cols_new <- c("Departed_Date", "Unique_Identifier")

merge_1_2 <- merge_dfs(df1_trimmed, df2_trimmed, df1_cols_old, df1_cols_new, character(0), character(0))

df12 <- merge_1_2$df_merged|>
  mutate(Unique_Identifier = coalesce(Anonymized_Identifer, Unique_Identifier))|>
  select(-Anonymized_Identifer)

venn_12_4 <- inspect_columns(names(df12), names(df4_trimmed))
df12_cols_old <- c("Latest_Arrest_Program_Current", "Latest_Arrest_Program_Current_Code", "Latest_Person_Apprehension_Date", "Latest_Person_Departed_Date")
df12_cols_new <- c("Latest_Arrest_Program", "Latest_Arrest_Program_Code", "Latest_Arrest_Apprehension_Date", "Most_Recent_Prior_Depart_Date")
df4_cols_old <- c("Port_Of_Departure", "Departed_To_Country", "Country_of_Birth", "Country_of_Citizenship", "Year_of_Birth", "Rc_Threat_Level", "Unique_ID")
df4_cols_new <- c("Port_of_Departure", "Departure_Country", "Birth_Country", "Citizenship_Country", "Birth_Year", "Case_Threat_Level", "Unique_Identifier")

merge_all <- merge_dfs(df12, df4_trimmed, df12_cols_old, df12_cols_new, df4_cols_old, df4_cols_new)
removals_df <- merge_all$df_merged


# check final dataframe
all_weekly_counts_trimmed <- get_weekly_counts(removals_df, "Departed_Date")

ggplot(all_weekly_counts_trimmed ,aes(week_start, y=n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

# FLAG Duplicates 
removals_df <- removals_df |>
  janitor::clean_names(allow_dupes = FALSE)|>
  mutate(departed_date = as.Date(departed_date))

setDT(removals_df)
setorder(removals_df, unique_identifier, departed_date)

removals_df[,
  `:=`(
    hours_since_last = as.numeric(
      departed_date - shift(departed_date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(departed_date, type = "lead") - departed_date,
      units = "hours"
    )
  ),
  by = unique_identifier
]

removals_df <-
  removals_df |>
  as_tibble() |>
  mutate(
    within_24hrs_prior = !is.na(hours_since_last) & hours_since_last <= 24,
    within_24hrs_next  = !is.na(hours_until_next) & hours_until_next <= 24,

    duplicate_likely = case_when(
      !is.na(unique_identifier) ~ within_24hrs_prior | within_24hrs_next,
      TRUE ~ FALSE
    ),

    # NEW: which rows to drop (drop the later row in a <=24hr pair)
    drop_row = case_when(
      is.na(unique_identifier) ~ FALSE,
      within_24hrs_prior ~ TRUE,   # later-than-previous within 24h => drop
      TRUE ~ FALSE
    )
  ) |>
  select(
    -within_24hrs_prior,
    -within_24hrs_next,
    -hours_since_last,
    -hours_until_next
  )

write_feather(removals_df, "data/ice-final/removals-final-with-flags.feather")

