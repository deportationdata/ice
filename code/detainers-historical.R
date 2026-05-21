# Source-file selection
# | df  | folder              | window                   | role            | reason                                       |
# |-----|---------------------|--------------------------|-----------------|----------------------------------------------|
# | df3 | npr                 | < 2017-06-18             | use             | only source with pre-2017 coverage           |
# | df1 | 2025-ICFO-18038     | 2017-06-18 .. 2025-03-15 | use             | mid-window coverage                          |
# | df2 | 120125              | >= 2025-03-16            | use             | most recent release                          |
# | df4 | November 2025 Release| (whole)                 | loaded, unused  | currently kept for reference only            |

# --- Packages ---
library(readxl)
library(readr)
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
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
# ROOT: ice/
# use the OLD folder parser that doesn't guess and converts everything to character and then guess the columns later...
df1 <- get_folder_df0(
  folder_dir = "inputs/detainers/2025-ICFO-18038",
  pattern = "\\.xlsx$",
  recursive = TRUE,
  anchor_idx = 2
)|>
  mutate(source_file = "2025-ICFO-18038")|>
  select(where(is_not_blank_or_redacted))|>
  mutate(across(ends_with("_Date"), as.Date))

df2 <- list.files(
    path = "inputs/detainers/120125",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  map_dfr(\(fp) {
    excel_sheets(fp) |>
      map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
  }) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(source_file = "120125") |>
  select(where(is_not_blank_or_redacted))

# --- For npr, there are .csv and .txt files so the folder parsers won't work ---
npr_csv_files <- list.files(
  path = "inputs/detainers/npr",
  pattern = "\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)
npr_txt_files <- list.files(
  path = "inputs/detainers/npr",
  pattern = "\\.txt$",
  recursive = TRUE,
  full.names = TRUE
)

# ---------- Helpers ----------
parse_date_like <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, c("POSIXct", "POSIXlt"))) return(as.Date(x))

  x_chr <- str_trim(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN")] <- NA_character_

  as.Date(
    x_chr,
    tryFormats = c(
      "%Y-%m-%d",  # ISO
      "%m/%d/%y",  # 2-digit year first (prevents 0009 issue)
      "%m/%d/%Y"   # 4-digit year
    )
  )
}

batch_to_date <- function(df, cols, verbose = TRUE) {
  cols <- intersect(cols, names(df))

  out <- df |>
    mutate(across(all_of(cols), parse_date_like))

  if (verbose && length(cols) > 0) {
    walk(cols, \(col) {
      converted_n <- sum(!is.na(out[[col]]))
      cat(sprintf(
        "Column: %-35s | Converted: %7d / %7d\n",
        col, converted_n, nrow(out)
      ))
    })
  }

  out
}

# ---------- Read NPR files ----------
# .csv files are tab-delimited; .txt files are comma-delimited
npr_a <- map_dfr(npr_csv_files, read_tsv,
                 col_types = cols(.default = col_character()), name_repair = "minimal")
npr_b <- map_dfr(npr_txt_files[1:5], read_csv,
                 col_types = cols(.default = col_character()), name_repair = "minimal")
npr_c <- read_csv(npr_txt_files[6],
                  col_types = cols(.default = col_character()), name_repair = "minimal")

# Merge npr pieces since these are all from npr
npr_merge <- bind_rows(npr_a, npr_b, npr_c)
rm(npr_a, npr_b, npr_c)
gc()

# ---------- Date conversion ----------
date_cols <- names(npr_merge) |>
  keep(\(nm) stringr::str_detect(nm, stringr::regex("Date", ignore_case = TRUE))) |>
  setdiff(c("Birth_Date", "Birth Date"))

df3 <- batch_to_date(npr_merge, date_cols) |>
  rename_with(\(nms) {
    nms |>
      str_replace_all("\\s+", "_") |>
      make_clean_names(case = "none") |>
      str_replace_all("_+", "_") |>
      str_split("_") |>
      lapply(\(parts) paste(str_to_title(str_to_lower(parts)), collapse = "_")) |>
      unlist()
  })|>
  mutate(source_file = "npr")
rm(npr_merge, date_cols)
gc()

df4 <- list.files(
    path = "inputs/detainers/November 2025 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  map_dfr(\(fp) {
    excel_sheets(fp) |>
      map_dfr(\(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000))
  }) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(source_file = "November 2025 Release") |>
  select(where(is_not_blank_or_redacted))

# # --- Weekly counts for source-coverage check ---
# df1_weekly_counts <- get_weekly_counts(df1, "Detainer_Prepare_Date")
# df2_weekly_counts <- get_weekly_counts(df2, "Detainer_Prepare_Date")
# df3_weekly_counts <- get_weekly_counts(df3, "Prepare_Date")
# df4_weekly_counts <- get_weekly_counts(df4, "Detainer_Prepare_Date")

# all_weekly_counts <- bind_rows(df1_weekly_counts, df2_weekly_counts, df3_weekly_counts, df4_weekly_counts)

# ggplot(all_weekly_counts, aes(week_start, n, color = source_file))+
#   geom_line(alpha = 0.5)+
#   theme_minimal()

# boundaries <- all_weekly_counts |>
#   group_by(source_file) |>
#   summarise(start = min(week_start), end = max(week_start))

# trim datasets by date and merge (npr, 2025-ICFO-18038, 120125)
df3_trimmed <- df3 |>
  filter(Prepare_Date < as.Date("2017-06-18"))
df1_trimmed <- df1 |>
  filter(Detainer_Prepare_Date >= as.Date("2017-06-18") & Detainer_Prepare_Date < as.Date("2025-03-16"))
df2_trimmed <- df2 |>
  filter(Detainer_Prepare_Date >= as.Date("2025-03-16"))

# Drop the untrimmed originals; df4 stays (used after merge for completeness, but not currently — keep ref)
rm(list = setdiff(ls(), c("df1_trimmed", "df2_trimmed", "df3_trimmed", "df4")))
gc()

source("code/functions/safe_bind_rows.R")

# Step 2. Rename the columns that are probably mergeable (i.e., similar names for the same field)
df12 <- df1_trimmed |>
  rename(
    Multiple_Prior_MISD_Yes_No = Multiple_Prior_Misd_Yes_No,
    Final_Program = Program,
    Most_Serious_Conviction_Charge_Code = Detainer_Most_Serious_Conviction_Charge_Code,
    Most_Serious_Conviction_Charge = Detainer_Most_Serious_Conviction_Charge,
    Most_Serious_Conviction_Conviction_Date = Detainer_Most_Serious_Conviction_Conviction_Date,
    Most_Serious_Conviction_Sentence_Days = Detainer_Most_Serious_Conviction_Sentence_Days,
    Most_Serious_Conviction_Sentence_Months = Detainer_Most_Serious_Conviction_Sentence_Months,
    Most_Serious_Conviction_Sentence_Years = Detainer_Most_Serious_Conviction_Sentence_Years,
    Unique_Identifier = Alien_Number_Unique_Identifier,
    Most_Serious_Conviction_Charge_Date = Detainer_Most_Serious_Conviction_Charge_Date,
    Detention_Facility = Detainer_Detention_Facility,
    Detention_Facility_Code = Detainer_Detention_Facility_Code
  ) |>
  safe_bind_rows(
    df2_trimmed |> rename(
      Detainer_Threat_Level = Detainer_Prep_Threat_Level,
      Most_Serious_Conviction_Charge_Code = MSC_Charge_Code,
      Most_Serious_Conviction_Charge = Most_Serious_Conviction_MSC_Charge,
      Most_Serious_Conviction_Conviction_Date = MSC_Conviction_Date,
      Most_Serious_Conviction_Sentence_Days = MSC_Sentence_Days,
      Most_Serious_Conviction_Sentence_Months = MSC_Sentence_Months,
      Most_Serious_Conviction_Sentence_Years = MSC_Sentence_Years,
      Most_Serious_Conviction_Charge_Date = MSC_Charge_Date
    )
  )

rm(df1_trimmed, df2_trimmed)
gc()

detainers_df <- df12 |>
  rename(Prepare_Date_Time = Detainer_Prepare_Date) |>
  mutate(Prepare_Date_Time = as.POSIXct(Prepare_Date_Time, tz = "UTC")) |>
  safe_bind_rows(
    df3_trimmed |>
      rename(
        Prepare_Date_Time = Prepare_Date,
        Detainer_Lift_Reason_Code2 = Detainer_Lift_Reason_2_Code,
        Order_to_Show_Cause_Served_Yes_No = Osc_Served_Yes_No,
        Order_to_Show_Cause_Served_Date = Osc_Served_Date,
        Detainer_AOR = Area_Of_Responsibility
      ) |>
      mutate(Prepare_Date_Time = as.POSIXct(Prepare_Date_Time, tz = "UTC"))
  ) |>
  select(-Detainer_Detention_Facility) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(prepare_date = as.Date(prepare_date_time))

rm(df12, df3_trimmed, df4)
gc()

# FLAG Duplicates
setDT(detainers_df)
setorder(detainers_df, unique_identifier, prepare_date_time)

detainers_df[,
  `:=`(
    hours_since_last = as.numeric(
      prepare_date_time - shift(prepare_date_time, type = "lag"),
      units = "hours"
    ),
    hours_until_next = as.numeric(
      shift(prepare_date_time, type = "lead") - prepare_date_time,
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

write_parquet(detainers_df, "data/detainers-historical.parquet", compression = "zstd", compression_level = 19)
