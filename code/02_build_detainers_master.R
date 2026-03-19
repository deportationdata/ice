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


df1 <- read_feather("data/ice-raw/detainers-selected/2025-ICFO-18038_combined.feather")
df2 <- read_feather("data/ice-raw/detainers-selected/120125_combined.feather")
df3 <- read_feather("data/ice-raw/detainers-selected/npr_combined.feather")
df4 <- read_feather("data/ice-raw/detainers-selected/nov2025_combined.feather")

df1_weekly_counts <- get_weekly_counts(df1, "Detainer_Prepare_Date")
df2_weekly_counts <- get_weekly_counts(df2, "Detainer_Prepare_Date")
df3_weekly_counts <- get_weekly_counts(df3, "Prepare_Date")
df4_weekly_counts <- get_weekly_counts(df4, "Detainer_Prepare_Date")

all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts)

ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

boundaries <- all_weekly_counts |> 
  group_by(source_file) |> 
  summarise(start = min(week_start), end = max(week_start))

# trim datasets by date and merge (npr, 2025-ICFO-18038, 120125)
df3_trimmed <- df3 |>
  filter(Prepare_Date < as.Date("2017-06-18"))
df1_trimmed <- df1 |>
  filter(Detainer_Prepare_Date >= as.Date("2017-06-18") & Detainer_Prepare_Date < as.Date("2025-03-16"))
df2_trimmed <- df2 |>
  filter(Detainer_Prepare_Date >= as.Date("2025-03-16"))

venn_1_2b <- inspect_columns(names(df1), names(df2)) 
# --- There appears to be 13 overlapping columns between df1 and df2 
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

merge_1_2 <- merge_dfs(df1_trimmed, df2_trimmed,
                         df1_cols_old, df1_cols_new,
                         df2_cols_old, df2_cols_new)

df12 <- merge_1_2$df_merged

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
merge_12_3 <- merge_dfs(df12, df3_trimmed,
                          df12_cols_old, df12_cols_new,
                          df3_cols_old, df3_cols_new)

detainers_df <- merge_12_3$df_merged |>
  select(-Detainer_Detention_Facility)

# check final dataframe
all_weekly_counts_trimmed <- get_weekly_counts(detainers_df, "Prepare_Date")

ggplot(all_weekly_counts_trimmed ,aes(week_start, y=n, color = source_file))+
  geom_line(alpha = 0.5)+
  theme_minimal()

# FLAG Duplicates 
detainers_df <- detainers_df |>
  janitor::clean_names(allow_dupes = FALSE)|>
  mutate(prepare_date = as.Date(prepare_date))

setDT(detainers_df)
setorder(detainers_df, unique_identifier, prepare_date)

detainers_df[,
  `:=`(
    hours_since_last = as.numeric(
      prepare_date - shift(prepare_date, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(prepare_date, type = "lead") - prepare_date,
      units = "hours"
    )
  ),
  by = unique_identifier
]

detainers_df <-
  detainers_df |>
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

write_feather(detainers_df, "data/ice-final/detainers-final-with-flags.feather")

