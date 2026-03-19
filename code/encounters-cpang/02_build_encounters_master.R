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


df1 <- read_feather("data/ice-raw/encounters-selected/f082025_combined.feather")
df2 <- read_feather("data/ice-raw/encounters-selected/uwchr_combined.feather")

df1_weekly_counts <- get_weekly_counts(df1, "Event_Date")
df2_weekly_counts <- get_weekly_counts(df2, "Event_Date")

all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts)|>
  arrange(week_start)

ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

boundaries <- all_weekly_counts |> 
  group_by(source_file) |> 
  summarise(start = min(week_start), end = max(week_start))

# MERGE DFS
venn_1_2b <- inspect_columns(names(df1), names(df2)) 

# Step 2: Merge columns 
df2_cols_old <- c("Area_of_Responsibility", "Landmark")
df2_cols_new <- c("Responsible_AOR", "Event_Landmark")

merge_1_2 <- merge_dfs(df1, df2, character(0), character(0), df2_cols_old, df2_cols_new)

encounters_df <- merge_1_2$df_merged
# check final dataframe
all_weekly_counts_trimmed <- get_weekly_counts(encounters_df, "Event_Date")

ggplot(all_weekly_counts_trimmed ,aes(week_start, y=n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

# FLAG Duplicates 
encounters_df <- encounters_df |>
  janitor::clean_names(allow_dupes = FALSE)|>
  mutate(event_date = as.Date(event_date))

setDT(encounters_df)
setorder(encounters_df, unique_identifier, event_date)

encounters_df[,
  `:=`(
    hours_since_last = as.numeric(
      event_date - shift(event_date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(event_date, type = "lead") - event_date,
      units = "hours"
    )
  ),
  by = unique_identifier
]

encounters_df <-
  encounters_df |>
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

write_feather(encounters_df, "data/ice-final/encounters-final-with-flags.feather")

