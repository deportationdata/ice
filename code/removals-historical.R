# Source-file selection
# | df  | folder           | window                   | role           | reason                                |
# |-----|------------------|--------------------------|----------------|---------------------------------------|
# | df4 | 14-03290         | < 2013-09-29             | use            | only source with pre-2013 coverage    |
# | df1 | 2023_ICFO_42034  | 2013-09-29 .. 2023-09-23 | use            | mid-window coverage                   |
# | df2 | 082025           | >= 2023-09-24            | use            | most recent release                   |
# | df3 | uwchr            | (whole)                  | loaded, unused | not used downstream                   |

# --- Packages ---
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)
library(stringdist)
library(arrow)
library(ggplot2)
library(data.table)
library(tidylog)

# --- Source Functions ---
source("code/functions/process_folder_data.R")
source("code/functions/is_not_blank_or_redacted.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
# ROOT: ice/
df1 <- get_folder_df0(
  folder_dir = "inputs/removals/2023_ICFO_42034",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2023_ICFO_42034")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df2 <- get_folder_df0(
  folder_dir = "inputs/removals/082025",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "082025")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df3 <- get_folder_df0(
  folder_dir = "inputs/removals/uwchr",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "uwchr")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

# --- df4 needs separate processing ---
get_folder_df2 <- function(folder_dir, pattern, recursive, anchor_idx, sheet_n = 2){
  files <- list_files_in_dir(dir = folder_dir, pattern = pattern, recursive = recursive) #df4_files
  folder_df <- tibble::tibble()
  for (f in files){
    sheet_df <- read_excel(f, sheet = sheet_n, guess_max = 10000) # read the nth sheet
    processed_df <- process_sheet(sheet_df, anchor_idx = anchor_idx)
    folder_df <- bind_rows(folder_df, processed_df)
    print(head(processed_df))
  }
  # Convert temporal columns
  folder_df2 <- convert_df_temporal_columns(folder_df)
  return(folder_df2)
}

df4 <- get_folder_df2(
  folder_dir = "inputs/removals/14-03290",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2,
  sheet_n = 2
)|>
  mutate(source_file = "14-03290")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

# # --- Weekly counts for source-coverage check ---
# df1_weekly_counts <- get_weekly_counts(df1, "Departure_Date")
# df2_weekly_counts <- get_weekly_counts(df2, "Departed_Date")
# df3_weekly_counts <- get_weekly_counts(df3, "Departed_Date")
# df4_weekly_counts <- get_weekly_counts(df4, "Departed_Date")

# all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts)

# ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
#   geom_line(alpha = 0.5)+
#   theme_minimal()

# boundaries <- all_weekly_counts |>
#   group_by(source_file) |>
#   summarise(start = min(week_start), end = max(week_start))

# trim datasets by date and merge (14-03290, 2024_ICFO_42034, 082025)
df4_trimmed <- df4 |>
  filter(Departed_Date < as.Date("2013-09-29"))
df1_trimmed <- df1 |>
  filter(Departure_Date >= as.Date("2013-09-29") & Departure_Date < as.Date("2023-09-24"))
df2_trimmed <- df2 |>
  filter(Departed_Date >= as.Date("2023-09-24"))

# Drop the untrimmed originals and df3 (not used downstream)
rm(list = setdiff(ls(), c("df1_trimmed", "df2_trimmed", "df4_trimmed")))
gc()

source("code/functions/safe_bind_rows.R")

df12 <- df1_trimmed |>
  rename(
    Departed_Date_Time = Departure_Date,
    Unique_Identifier  = Anonymized_Identifier
  ) |>
  mutate(Departed_Date_Time = as.POSIXct(Departed_Date_Time, tz = "UTC")) |>
  safe_bind_rows(
    df2_trimmed |>
      rename(Departed_Date_Time = Departed_Date) |>
      mutate(Departed_Date_Time = as.POSIXct(Departed_Date_Time, tz = "UTC"))
  ) |>
  mutate(Unique_Identifier = coalesce(Anonymized_Identifer, Unique_Identifier)) |>
  select(-Anonymized_Identifer)

rm(df1_trimmed, df2_trimmed)
gc()

removals_df <- df12 |>
  rename(
    Latest_Arrest_Program = Latest_Arrest_Program_Current,
    Latest_Arrest_Program_Code = Latest_Arrest_Program_Current_Code,
    Latest_Arrest_Apprehension_Date = Latest_Person_Apprehension_Date,
    Most_Recent_Prior_Depart_Date = Latest_Person_Departed_Date
  ) |>
  safe_bind_rows(
    df4_trimmed |>
      rename(
        Departed_Date_Time  = Departed_Date,
        Port_of_Departure   = Port_Of_Departure,
        Departure_Country   = Departed_To_Country,
        Birth_Country       = Country_of_Birth,
        Citizenship_Country = Country_of_Citizenship,
        Birth_Year          = Year_of_Birth,
        Case_Threat_Level   = Rc_Threat_Level,
        Unique_Identifier   = Unique_ID
      ) |>
      mutate(Departed_Date_Time = as.POSIXct(Departed_Date_Time, tz = "UTC"))
  ) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(departed_date = as.Date(departed_date_time))

rm(df12, df4_trimmed)
gc()

# FLAG Duplicates
setDT(removals_df)
setorder(removals_df, unique_identifier, departed_date_time)

removals_df[,
  `:=`(
    hours_since_last = as.numeric(
      departed_date_time - shift(departed_date_time, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(departed_date_time, type = "lead") - departed_date_time,
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

write_parquet(removals_df, "data/removals-historical.parquet", compression = "zstd", compression_level = 19)
