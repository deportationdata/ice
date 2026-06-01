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
source("code/functions/process_folder_data.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/if_has.R")
source("code/functions/coalesce_rename.R")
source("code/functions/save_historical_outputs.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

# --- Build dataframes directly from folders (no intermediate parquet save) ---
# ROOT: ice/

col_type_overrides_2025_ICFO_18038_detainers <- c(
  all_text(c(
    "Detainer_Lift_Reason", "Detainer_Type", "Detainer_Lift_Reason_Code",
    "Detainer_Lift_Reason2", "Detainer_Lift_Reason_Code2",
    "Arrest_Warrant_Served_Yes_No", "Deportation_Ordered_Yes_No",
    "Active_Investigation_Yes_No", "Order_to_Show_Cause_Served_Yes_No",
    "Biometric_Match_Yes_No", "Statements_Made_Yes_No", "Prior_Felony_Yes_No",
    "Multiple_Prior_Misd_Yes_No", "Violent_Misdemeanor_Yes_No",
    "Illegal_Entry_Yes_No", "Illegal_Reentry_Yes_No", "Immigration_Fraud_Yes_No",
    "Significant_Risk_Yes_No", "Other_Removal_Reason_Yes_No", "Other_Removal_Reason",
    "Aggravated_Felony_Yes_No", "Criminal_Street_Gang_Yes_No",
    "Detainer_Request_Upon_Conviction_Yes_No", "Resume_Custody_Yes_No",
    "Component_Notified", "Previous_Detainer_Cancel_Yes_No",
    "Unlawful_Entry_Yes_No", "Unlawful_Attempt_Yes_No", "Final_Order_Yes_No",
    "Federal_Interest_Yes_No", "Visa_Yes_No",
    "Death_Transfer_Notify_Request_Yes_No", "Federal_Register_Notice_Yes_No",
    "Notify_Release_Request_Yes_No", "Request_Acceptance_Yes_No",
    "Return_Signature_Request_Yes_No", "Return_Envelope_Include_Yes_No",
    "Return_by_Fax_Yes_No", "Detainer_AOR", "Program", "Active_Yes_No",
    "Detainer_Detention_Facility_Code", "Government_Employee_ID",
    "Return_to_Employee_ID", "Detainer_Inmate_Number_Type_Code",
    "Detainer_Facility_Other_Type", "Other_Detainer_Facility_Name",
    "Projected_Release_Day", "Projected_Release_Month",
    "Detainer_Detention_Facility", "Detainer_Facility_Function",
    "Detainer_Facility_Type", "Apprehension_AOR", "Apprehension_Site",
    "Apprehension_Final_Program", "Apprehension_Method",
    "Apprehension_Program_Group", "Port_of_Departure", "Departure_Country",
    "Case_Category", "Gender", "Birth_Country", "Birth_City", "Birth_State",
    "Citizenship_Country", "Birth_Date", "Race", "Ethnicity", "Admission_Class",
    "Student_Violator_Yes_No", "Non_Immigrant_Status_Violation_Yes_No",
    "Non_Immigrant_Overstay_Yes_No", "EO_VISA_Abuse_Flag",
    "EO_Person_Visa_Abuse_Yes_No", "EO_Gang_Flag", "EO_Person_Gang_Flag",
    "Entry_Status", "Latest_Entry_Status",
    "Detainer_Most_Serious_Conviction_Charge_Code",
    "Detainer_Most_Serious_Conviction_Charge",
    "Detainer_Most_Serious_Conviction_Crime_Class",
    "Detainer_Most_Serious_Conviction_Criminal_Charge_Status",
    "Detainer_Most_Serious_Charge_Code", "Detainer_Most_Serious_Charge",
    "Detainer_Most_Serious_Charge_Crime_Class",
    "Detainer_Most_Serious_Criminal_Charge_Status",
    "Detainer_Most_Serious_Pending_Charge_Code",
    "Detainer_Most_Serious_Pending_Charge",
    "Detainer_Most_Serious_Pending_Criminal_Charge_Status",
    "Detainer_Most_Serious_Pending_Crime_Class",
    "Aggravated_Felon_Type", "Mandatory_Detention_Yes_No",
    "Detainer_Prepared_Criminality", "Detainer_Threat_Level",
    "Processing_Disposition", "Latest_Detainer_Yes_No",
    "Detainer_Removal_Case_Yes_No", "Current_Yes_No",
    "Apprehension_Site_Landmark", "EID_DTA_ID", "Detainer_ID",
    "EID_Subject_ID", "EID_Civilian_ID", "EID_Person_ID", "EID_Case_ID",
    "Alien_Number_Unique_Identifier", "Alien_File_Number", "Subject_Name",
    "Apprehension_Program"
  )),
  Detainer_Prepare_Date                             = "date",
  Detainer_Lift_Date                                = "date",
  Previous_Detainer_Cancel_Prep_Date                = "date",
  Order_to_Show_Cause_Served_Date                   = "date",
  Arrest_Warrant_Served_Date                        = "date",
  EID_DTA_Create_Date                               = "date",
  Projected_Release_Date                            = "date",
  Apprehension_Date                                 = "date",
  Departed_Date                                     = "date",
  Final_Order_Date                                  = "date",
  Entry_Date                                        = "date",
  Latest_Entry_Date                                 = "date",
  Detainer_Most_Serious_Conviction_Conviction_Date  = "date",
  Detainer_Most_Serious_Conviction_Charge_Date      = "date",
  Detainer_Most_Serious_Charge_Date                 = "date",
  Most_Serious_Charge_Conviction_Date               = "date",
  Detainer_Most_Serious_Pending_Conviction_Date     = "date",
  Detainer_Most_Serious_Pending_Charge_Date         = "date",
  Detainer_Prepare_Fiscal_Year                      = "numeric",
  Projected_Release_Year                            = "numeric",
  Birth_Year                                        = "numeric",
  Detainer_Most_Serious_Conviction_Sentence_Days    = "numeric",
  Detainer_Most_Serious_Conviction_Sentence_Months  = "numeric",
  Detainer_Most_Serious_Conviction_Sentence_Years   = "numeric",
  Detainer_Most_Serious_Charge_Sentence_Days        = "numeric",
  Detainer_Most_Serious_Charge_Sentence_Months      = "numeric",
  Detainer_Most_Serious_Charge_Sentence_Years       = "numeric",
  Detainer_Most_Serious_Pending_Sentence_Days       = "numeric",
  Detainer_Most_Serious_Pending_Sentence_Months     = "numeric",
  Detainer_Most_Serious_Pending_Sentence_Years      = "numeric"
)

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
          \(sh) process_sheet(
            file_path = fp, sheet = sh, anchor_idx = 2,
            col_type_overrides = col_type_overrides_2025_ICFO_18038_detainers
          ),
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
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  filter(Detainer_Prepare_Date >= as.Date("2017-06-18") & Detainer_Prepare_Date < as.Date("2025-03-16"))

col_type_overrides_march2026_detainers <- c(
  Detainer_Prepare_Date             = "date",
  Facility_State                    = "text",
  Facility_AOR                      = "text",
  Port_of_Departure                 = "text",
  Departure_Country                 = "text",
  Departed_Date                     = "date",
  Case_Status                       = "text",
  Detainer_Criminality              = "text",
  Detainer_Facility                 = "text",
  Detainer_Facility_Code            = "text",
  Facility_City                     = "text",
  Detainer_Threat_Level             = "text",
  Gender                            = "text",
  Citizenship_Country               = "text",
  Birth_Country                     = "text",
  Birth_Year                        = "numeric",
  Entry_Status                      = "text",
  MSC_Charge                        = "text",
  MSC_Sentence_Days                 = "numeric",
  MSC_Sentence_Months               = "numeric",
  MSC_Sentence_Years                = "numeric",
  MSC_Charge_Code                   = "text",
  MSC_Charge_Date                   = "date",
  MSC_Conviction_Date               = "date",
  Aggravated_Felon_Yes_No           = "text",
  Processing_Disposition            = "text",
  Case_Category                     = "text",
  TOA_Case_Category                 = "text",
  TOA_Current_Program               = "text",
  Apprehension_Method               = "text",
  Final_Order_Yes_No                = "text",
  Final_Order_Date                  = "date",
  Apprehension_Date                 = "date",
  Entry_Date                        = "date",
  Prior_Felony_Yes_No               = "text",
  Multiple_Prior_MISD_Yes_No        = "text",
  Violent_Misdemeanor_Yes_No        = "text",
  Illegal_Entry_Yes_No              = "text",
  Illegal_Reentry_Yes_No            = "text",
  Immigration_Fraud_Yes_No          = "text",
  Significant_Risk_Yes_No           = "text",
  Other_Removal_Reason_Yes_No       = "text",
  Other_Removal_Reason              = "text",
  Criminal_Street_Gang_Yes_No       = "text",
  Aggravated_Felony_Yes_No          = "text",
  Deportation_Ordered_Yes_No        = "text",
  Order_to_Show_Cause_Served_Yes_No = "text",
  Order_to_Show_Cause_Served_Date   = "date",
  Biometric_Match_Yes_No            = "text",
  Statements_Made_Yes_No            = "text",
  Unlawful_Attempt_Yes_No           = "text",
  Unlawful_Entry_Yes_No             = "text",
  Visa_Yes_No                       = "text",
  Federal_Register_Notice_Yes_No    = "text",
  Resume_Custody_Yes_No             = "text",
  Detainer_Lift_Reason              = "text",
  Detainer_Lift_Reason_Code         = "text",
  Active_Investigation_Yes_No       = "text",
  Arrest_Warrant_Served_Date        = "date",
  Detainer_Type                     = "text",
  Notify_Release_Request_Yes_No     = "text",
  TOD_Current_Duty_Site             = "text",
  EID_DTA_ID                        = "text",
  Anonymized_Identifier             = "text"
)

df2 <- list.files(
    path = "inputs/detainers/March 2026 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) process_sheet(
        file_path = fp,
        sheet = sh,
        anchor_idx = 2,
        guess_max = 10000,
        col_type_overrides = col_type_overrides_march2026_detainers
      ),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(
    row_original = as.integer(row_number()),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  filter(Detainer_Prepare_Date >= as.Date("2025-03-16"))

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
  }) |>
  filter(Prepare_Date < as.Date("2017-06-18"))
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

source("code/functions/safe_bind_rows.R")

# Step 2. Rename the columns that are probably mergeable (i.e., similar names for the same field)
df12 <- df1 |>
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
    df2 |> rename(
      Unique_Identifier = Anonymized_Identifier
    ) |> rename(any_of(c(
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

rm(df1, df2)
gc()

detainers_df <- df12 |>
  rename(Prepare_Date = Detainer_Prepare_Date) |>
  safe_bind_rows(
    df3 |> rename(
      Detainer_Lift_Reason_Code2 = Detainer_Lift_Reason_2_Code,
      Order_to_Show_Cause_Served_Yes_No = Osc_Served_Yes_No,
      Order_to_Show_Cause_Served_Date = Osc_Served_Date,
      Detainer_AOR = Area_Of_Responsibility
    )
  ) |>
  select(-Detainer_Detention_Facility) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  mutate(prepare_date = as.Date(prepare_date)) |>
  make_redactions_na() |>
  mutate(across(any_of("birth_year"), as.integer))

detainers_df <- detainers_df |>
  coalesce_rename("detainer_prepared_criminality", "detainer_criminality") |>
  coalesce_rename("detainer_prep_threat_level",    "detainer_threat_level") |>
  coalesce_rename("felon",                         "aggravated_felon_yes_no") |>
  coalesce_rename("arrest_time_case_category",     "toa_case_category") |>
  coalesce_rename("arrest_time_current_program",   "toa_current_program") |>
  coalesce_rename("federal_interest_yes_no",       "federal_register_notice_yes_no")

rm(df12, df3)
gc()

# ---- Construct Duplicate Episode Indicators ----
setDT(detainers_df)

detainers_df[,
  unique_identifier_nona := fifelse(
    is.na(unique_identifier),
    paste0("noid_", .I),
    unique_identifier
  )
]

setorder(
  detainers_df,
  unique_identifier_nona,
  prepare_date,
  file_original,
  sheet_original,
  row_original
)

detainers_df[,
  duplicate_episode_id := {
    gap <- as.numeric(
      prepare_date - shift(prepare_date, type = "lag"),
      units = "hours"
    )
    cumsum(is.na(gap) | gap > 24)
  },
  by = unique_identifier_nona
]

detainers_df[, unique_identifier_nona := NULL]

detainers_df <- as_tibble(detainers_df)

detainers_df |>
  col_vals_expr(
    expr = expr(!if_any(where(is.character), is_redacted)),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
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
  invisible()

save_historical_outputs(detainers_df, "detainers-historical")
