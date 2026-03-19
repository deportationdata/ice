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

# --- Read in Combined Data ---
df1 <- read_feather("data/ice-raw/arrests-selected/2022-ICFO-22955_combined.feather")
df2 <- read_feather("data/ice-raw/arrests-selected/2023_ICFO_42034_combined.feather")
df3 <- read_feather("data/ice-raw/arrests-selected/120125_combined.feather")
df4 <- read_feather("data/ice-raw/arrests-selected/uwchr_combined.feather")
df5 <- read_feather("data/ice-raw/arrests-selected/nov2025_combined.feather")

df1_weekly_counts <- get_weekly_counts(df1, "Apprehension_Date_And_Time")
df2_weekly_counts <- get_weekly_counts(df2, "Apprehension_Date")
df3_weekly_counts <- get_weekly_counts(df3, "Apprehension_Date")
df4_weekly_counts <- get_weekly_counts(df4, "Arrest_Date")
df5_weekly_counts <- get_weekly_counts(df5, "Apprehension_Date")

all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts, df5_weekly_counts)

ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

boundaries <- all_weekly_counts |> 
  group_by(source_file) |> 
  summarise(start = min(week_start), end = max(week_start))

# we use 2023_ICFO_42034 (df2) and November 2025 Release (df5)
df2_trimmed <- df2 |>
  filter(Apprehension_Date < as.Date("2023-09-24"))
df5_trimmed <- df5 |>
  filter(Apprehension_Date >= as.Date("2023-09-24"))

venn_2_5 <- inspect_columns(names(df2_trimmed), names(df5_trimmed))
df2_cols_old <- c("Anonymized_Identifier")
df2_cols_new <- c("Unique_Identifier")
merge_2_5 <- merge_dfs(df2_trimmed, df5_trimmed, df2_cols_old, df2_cols_new, character(0), character(0))

arrests_df <- merge_2_5$df_merged |>
  janitor::clean_names(allow_dupes = FALSE)|>
  mutate(apprehension_date = as.Date(apprehension_date))


# ---- Construct Duplicates Indicator ----
# two (or more) arrests within 24 hours of each other
setDT(arrests_df)
setorder(arrests_df, unique_identifier, apprehension_date)

arrests_df[,
  `:=`(
    hours_since_last = as.numeric(
      apprehension_date - shift(apprehension_date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(apprehension_date, type = "lead") - apprehension_date,
      units = "hours"
    )
  ),
  by = unique_identifier
]

arrests_df <-
  arrests_df |>
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

write_feather(arrests_df, "data/ice-final/arrests-final-with-flags.feather")

