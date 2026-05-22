# Source-file selection
# | df  | folder              | window                   | role            | reason                                       |
# |-----|---------------------|--------------------------|-----------------|----------------------------------------------|
# | df3 | npr                 | < 2017-06-18             | use             | only source with pre-2017 coverage           |
# | df1 | 2025-ICFO-18038     | 2017-06-18 .. 2025-03-15 | use             | mid-window coverage                          |
# | df2 | March 2026 Release  | >= 2025-03-16            | use             | most recent release                          |

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
library(pointblank)
library(haven)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/if_has.R")
source("code/functions/coalesce_rename.R")
source("code/functions/save_historical_outputs.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
# ROOT: ice/
# use the OLD folder parser that doesn't guess and converts everything to character and then guess the columns later...
source("code/functions/convert_temporal_columns.R")

df1 <- list.files(
    path = "inputs/detainers/2025-ICFO-18038",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) {
      excel_sheets(fp) |>
        set_names() |>
        map_dfr(
          \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000, force_col_type = "text"),
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  convert_df_temporal_columns() |>
  mutate(across(ends_with("_Date"), as.Date))

col_types_march2026_detainers_all <- c(
  "date",    # Detainer Prepare Date
  "text",    # Facility State
  "text",    # Facility AOR
  "text",    # Port of Departure
  "text",    # Departure Country
  "date",    # Departed Date
  "text",    # Case Status
  "text",    # Detainer Criminality
  "text",    # Detainer Facility
  "text",    # Detainer Facility Code
  "text",    # Facility City
  "text",    # Detainer Threat Level
  "text",    # Gender
  "text",    # Citizenship Country
  "text",    # Birth Country
  "text",    # Birth Date
  "numeric", # Birth Year
  "text",    # Entry Status
  "text",    # MSC Charge
  "numeric", # MSC Sentence Days
  "numeric", # MSC Sentence Months
  "numeric", # MSC Sentence Years
  "text",    # MSC Charge Code
  "date",    # MSC Charge Date
  "date",    # MSC Conviction Date
  "text",    # Aggravated Felon Yes No
  "text",    # Processing Disposition
  "text",    # Case Category
  "text",    # TOA Case Category
  "text",    # TOA Current Program
  "text",    # Apprehension Method
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "date",    # Apprehension Date
  "date",    # Entry Date
  "text",    # Prior Felony Yes No
  "text",    # Multiple Prior MISD Yes No
  "text",    # Violent Misdemeanor Yes No
  "text",    # Illegal Entry Yes No
  "text",    # Illegal Reentry Yes No
  "text",    # Immigration Fraud Yes No
  "text",    # Significant Risk Yes No
  "text",    # Other Removal Reason Yes No
  "text",    # Other Removal Reason
  "text",    # Criminal Street Gang Yes No
  "text",    # Aggravated Felony Yes No
  "text",    # Deportation Ordered Yes No
  "text",    # Order to Show Cause Served Yes No
  "date",    # Order to Show Cause Served Date
  "text",    # Biometric Match Yes No
  "text",    # Statements Made Yes No
  "text",    # Unlawful Attempt Yes No
  "text",    # Unlawful Entry Yes No
  "text",    # Visa Yes No
  "text",    # Federal Register Notice Yes No
  "text",    # Resume Custody Yes No
  "text",    # Detainer Lift Reason
  "text",    # Detainer Lift Reason Code
  "text",    # Active Investigation Yes No
  "date",    # Arrest Warrant Served Date
  "text",    # Detainer Type
  "text",    # Notify Release Request Yes No
  "text",    # TOD Current Duty Site
  "text",    # EID DTA ID
  "text"     # Anonymized Identifier
)
col_types_march2026_detainers_fy2324 <- col_types_march2026_detainers_all[-16]

df2 <- list.files(
    path = "inputs/detainers/March 2026 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) {
      n_cols <- ncol(read_excel(fp, n_max = 0, skip = 6))
      ct <- if (n_cols == length(col_types_march2026_detainers_all)) {
        col_types_march2026_detainers_all
      } else {
        col_types_march2026_detainers_fy2324
      }
      excel_sheets(fp) |> set_names() |> map_dfr(
        \(sh) read_excel(fp, sheet = sh, col_types = ct, skip = 6),
        .id = "sheet_original"
      )
    },
    .id = "file_original"
  ) |>
  rename_with(\(nms) nms |> str_replace_all("\\s+", "_") |> make_clean_names(case = "none")) |>
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))

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
npr_a <- map_dfr(
  set_names(npr_csv_files, \(p) file.path(basename(dirname(p)), basename(p))),
  read_tsv,
  col_types = cols(.default = col_character()),
  name_repair = "minimal",
  .id = "file_original"
)
npr_b <- map_dfr(
  set_names(npr_txt_files[1:5], \(p) file.path(basename(dirname(p)), basename(p))),
  read_csv,
  col_types = cols(.default = col_character()),
  name_repair = "minimal",
  .id = "file_original"
)
npr_c <- read_csv(
  npr_txt_files[6],
  col_types = cols(.default = col_character()),
  name_repair = "minimal"
) |>
  mutate(file_original = file.path(basename(dirname(npr_txt_files[6])), basename(npr_txt_files[6])))

# Merge npr pieces since these are all from npr
npr_merge <- bind_rows(npr_a, npr_b, npr_c) |>
  mutate(row_original = as.integer(row_number()), .by = "file_original")
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
  })
rm(npr_merge, date_cols)
gc()

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

# Drop the untrimmed originals
rm(df1, df2, df3)
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
    df2_trimmed |> rename(any_of(c(
      Detainer_Threat_Level = "Detainer_Prep_Threat_Level",
      Most_Serious_Conviction_Charge_Code = "MSC_Charge_Code",
      Most_Serious_Conviction_Charge = "Most_Serious_Conviction_MSC_Charge",
      Most_Serious_Conviction_Conviction_Date = "MSC_Conviction_Date",
      Most_Serious_Conviction_Sentence_Days = "MSC_Sentence_Days",
      Most_Serious_Conviction_Sentence_Months = "MSC_Sentence_Months",
      Most_Serious_Conviction_Sentence_Years = "MSC_Sentence_Years",
      Most_Serious_Conviction_Charge_Date = "MSC_Charge_Date"
    )))
  )

rm(df1_trimmed, df2_trimmed)
gc()

detainers_df <- df12 |>
  rename(Prepare_Date = Detainer_Prepare_Date) |>
  safe_bind_rows(
    df3_trimmed |> rename(
      Detainer_Lift_Reason_Code2 = Detainer_Lift_Reason_2_Code,
      Order_to_Show_Cause_Served_Yes_No = Osc_Served_Yes_No,
      Order_to_Show_Cause_Served_Date = Osc_Served_Date,
      Detainer_AOR = Area_Of_Responsibility
    )
  ) |>
  select(-Detainer_Detention_Facility) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(prepare_date = as.Date(prepare_date)) |>
  redact_to_na() |>
  mutate(across(any_of("birth_year"), as.integer))

detainers_df <- detainers_df |>
  coalesce_rename("detainer_prepared_criminality", "detainer_criminality") |>
  coalesce_rename("detainer_prep_threat_level",    "detainer_threat_level") |>
  coalesce_rename("felon",                         "aggravated_felon_yes_no") |>
  coalesce_rename("arrest_time_case_category",     "toa_case_category") |>
  coalesce_rename("arrest_time_current_program",   "toa_current_program") |>
  coalesce_rename("federal_interest_yes_no",       "federal_register_notice_yes_no")

rm(df12, df3_trimmed)
gc()

# FLAG Duplicates
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

detainers_df |>
  if_has("unique_identifier", \(d) col_vals_not_null(d, unique_identifier,
    actions = action_levels(warn_at = 0.85, stop_at = 0.95))) |>
  if_has("prepare_date", \(d) col_vals_not_null(d, prepare_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("prepare_date", \(d) col_vals_between(d, prepare_date,
    as.Date("2007-01-01"), Sys.Date(), na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("birth_year", \(d) col_vals_between(d, birth_year,
    1900L, as.integer(format(Sys.Date(), "%Y")), na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  if_has("gender", \(d) col_vals_in_set(d, gender,
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001))) |>
  if_has("final_order_yes_no", \(d) col_vals_in_set(d, final_order_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001))) |>
  if_has(c("duplicate_likely", "unique_identifier"), \(d) col_vals_not_null(d, duplicate_likely,
    preconditions = \(x) dplyr::filter(x, !is.na(unique_identifier)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01))) |>
  invisible()

save_historical_outputs(detainers_df, "detainers-historical")
