# Source-file selection — same as v1
# | df  | folder                      | window                   | role    | reason                                |
# |-----|-----------------------------|--------------------------|---------|---------------------------------------|
# | df6 | From-Emily-Excel-X-RIF      | 2010-05-30 .. 2014-10-11 | use     | only pre-2014 coverage                |
# | df5 | uwchr                       | 2014-10-12 .. 2023-12-30 | use     | mid-window coverage                   |
# | df4 | March 2026 Release          | >= 2023-12-31            | use     | most recent release                   |
# | df7 | From-Emily-FOIA-10-2554-527 | (whole)                  | use     | merged with df6 to fill gaps          |
#
# v2 difference vs v1: hard book-in trim is replaced with a cross-dataset
# matcher that collapses the same stint when it appears in both adjacent
# releases, and rescues boundary-crossing stints that only one release has.

library(readxl)
library(readr)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)
library(stringdist)
library(arrow)
library(data.table)
library(lubridate)
library(tidylog)
library(pointblank)
library(haven)

source("code/functions/process_folder_data_v2.R")
source("code/functions/convert_temporal_columns.R")
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/safe_bind_rows.R")
source("code/functions/save_historical_outputs.R")
source("code/functions/coalesce_rename.R")
source("code/functions/merge_boundary_crossers.R")

col_types_march2026_detentions <- c(
  "date", "date", "text", "date", "date", "text", "date", "text",
  "text", "text", "text", "text", "text", "text", "text", "text",
  "text", "text", "numeric", "numeric", "numeric", "text", "text",
  "text", "text", "date", "numeric", "text", "text", "text", "date",
  "text", "text", "text", "date", "text", "numeric", "date", "text",
  "text", "numeric", "text", "text", "date", "text", "date", "date",
  "text", "text", "text", "text", "text", "text"
)

df4 <- list.files(
    path = "inputs/detentions/March 2026 Release",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) read_excel(fp, sheet = sh, col_types = col_types_march2026_detentions, skip = 6),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  rename_with(\(nms) nms |> str_replace_all("\\s+", "_") |> make_clean_names(case = "none")) |>
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = c("file_original", "sheet_original")
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date)) |>
  mutate(across(ends_with("_Date"), as.Date))

df5 <- list.files(
    path = "inputs/detentions/uwchr",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) process_sheet(
        file_path = fp, sheet = sh, anchor_idx = 2,
        col_type_overrides = all_text(c(
          "Stay_Book_In_Date_Time", "Detention_Book_In_Date_And_Time",
          "Detention_Book_Out_Date_Time", "Stay_Book_Out_Date_Time",
          "Birth_Country_PER", "Birth_Country_ERO", "Citizenship_Country", "Race",
          "Ethnic", "Gender", "Birth_Date", "Birth_Year", "Entry_Date", "Entry_Status",
          "Most_Serious_Conviction_MSC_Criminal_Charge_Category",
          "MSC_Charge", "MSC_Charge_Code", "MSC_Conviction_Date",
          "MSC_Sentence_Days", "MSC_Sentence_Months", "MSC_Sentence_Years",
          "MSC_Crime_Class", "Case_Threat_Level", "Apprehension_Threat_Level",
          "Final_Program", "Detention_Facility_Code", "Detention_Facility",
          "Area_of_Responsibility", "Docket_Control_Office", "Detention_Release_Reason",
          "Stay_Release_Reason", "Alien_File_Number", "Anonymized_Identifier",
          "Citizenship_Country_Code", "Gender_Code",
          "History_Intake_DCO", "History_Detention_Facility",
          "History_Detention_Facility_Code", "Initial_Intake_Date",
          "History_Intake_Date", "History_Book_out_Date", "History_Release_Reason",
          "ERO_Apprehension_Date", "ERO_Apprehension_Landmark",
          "Initial_Intake_Detention_Facility", "Order_of_Detentions",
          "Unique_Person_ID", "Unique_Detention_Stay_ID"
        ))
      ),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  convert_df_temporal_columns() |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(ends_with("_Date"), as.Date))

df6 <- list.files(
    path = "inputs/detentions/From-Emily-Excel-X-RIF",
    pattern = "\\.xlsx$",
    recursive = TRUE,
    full.names = TRUE
  ) |>
  set_names(\(p) file.path(basename(dirname(p)), basename(p))) |>
  map_dfr(
    \(fp) excel_sheets(fp) |> set_names() |> map_dfr(
      \(sh) process_sheet(
        file_path = fp, sheet = sh, anchor_idx = 2,
        col_type_overrides = all_text(c(
          "Stay_Book_In_Date_Time", "Detention_Book_In_Date_And_Time",
          "Detention_Book_Out_Date_Time", "Stay_Book_Out_Date_Time",
          "Birth_Country_PER", "Birth_Country_ERO", "Citizenship_Country", "Race",
          "Ethnic", "Gender", "Birth_Date", "Birth_Year", "Entry_Date", "Entry_Status",
          "Most_Serious_Conviction_MSC_Criminal_Charge_Category",
          "MSC_Charge", "MSC_Charge_Code", "MSC_Conviction_Date",
          "MSC_Sentence_Days", "MSC_Sentence_Months", "MSC_Sentence_Years",
          "MSC_Crime_Class", "Case_Threat_Level", "Apprehension_Threat_Level",
          "Final_Program", "Detention_Facility_Code", "Detention_Facility",
          "Area_of_Responsibility", "Docket_Control_Office", "Detention_Release_Reason",
          "Stay_Release_Reason", "Alien_File_Number", "Anonymized_Identifier",
          "Citizenship_Country_Code", "Gender_Code",
          "History_Intake_DCO", "History_Detention_Facility",
          "History_Detention_Facility_Code", "Initial_Intake_Date",
          "History_Intake_Date", "History_Book_out_Date", "History_Release_Reason",
          "ERO_Apprehension_Date", "ERO_Apprehension_Landmark",
          "Initial_Intake_Detention_Facility", "Order_of_Detentions",
          "Unique_Person_ID", "Unique_Detention_Stay_ID"
        ))
      ),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  convert_df_temporal_columns() |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(ends_with("_Date"), as.Date))

df7_path <- "inputs/detentions/From-Emily-FOIA-10-2554-527/foia_10_2554_527_NoIDS.csv"
df7 <- read_csv(df7_path) |>
  clean_names(case = "upper_camel") |>
  rename_with(~ str_replace_all(.x, "(?<=[a-z])(?=[A-Z])", "_")) |>
  mutate(
    file_original = file.path(basename(dirname(df7_path)), basename(df7_path)),
    sheet_original = NA_character_,
    row_original = as.integer(row_number())
  ) |>
  mutate(across(ends_with("_Date"), ~ as.Date(mdy_hm(.x))))

# --- Harmonize column names so older + newer share matching keys ---

df67 <- df6 |>
  filter(History_Intake_Date > as.Date("2010-05-23") + 7) |>
  rename(
    Detention_Facility = History_Detention_Facility,
    Detention_Facility_Code = History_Detention_Facility_Code,
    Detention_Book_In_Date = History_Intake_Date,
    Detention_Book_Out_Date = History_Book_out_Date,
    Release_Reason = History_Release_Reason,
    Apprehension_Date = ERO_Apprehension_Date,
    Apprehension_Landmark = ERO_Apprehension_Landmark,
    Initial_Book_In_Facility = Initial_Intake_Detention_Facility,
    Facility_Held_In_Seq = Order_of_Detentions,
    Book_In_DCO = History_Intake_DCO
  ) |>
  mutate(Stay_Book_In_Date = as.Date(Initial_Intake_Date)) |>
  safe_bind_rows(
    df7 |> rename(
      Detention_Book_In_Date = Book_In_Date,
      Detention_Book_Out_Date = Book_Out_Date,
      Book_In_DCO = Book_In_Dco
    )
  ) |>
  rename(Detention_Release_Reason = Release_Reason)

df5_renamed <- df5 |>
  coalesce_rename("Ethnicity", "Ethnic") |>
  coalesce_rename("Most_Serious_Conviction_Charge_Code", "MSC_Charge_Code") |>
  coalesce_rename("Unique_Identifier", "Anonymized_Identifier") |>
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date_And_Time),
    Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date_Time),
    Stay_Book_In_Date = as.Date(Stay_Book_In_Date_Time),
    Birth_Country = coalesce(Birth_Country_ERO, Birth_Country_PER)
  ) |>
  select(-Birth_Country_ERO, -Birth_Country_PER) |>
  rename(Book_In_DCO = Docket_Control_Office)

df4_renamed <- df4 |>
  coalesce_rename("Detention_Book_In_Date_And_Time", "Book_In_Date_Time") |>
  coalesce_rename("Detention_Book_Out_Date_Time", "Book_Out_Date_Time") |>
  coalesce_rename("Most_Serious_Conviction_Charge_Code", "Most_Serious_Conviction_MSC_Charge_Code") |>
  coalesce_rename("Most_Serious_Conviction_Charge_Code", "MSC_Charge_Code") |>
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date_And_Time),
    Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date_Time),
    Stay_Book_In_Date = as.Date(Stay_Book_In_Date_Time)
  )

rm(df4, df5, df6, df7); gc()

# --- Merge boundary 1: df67 (older) <-> df5 (newer) at 2014-10-12 ---

merge1 <- merge_boundary_crossers(
  older = df67,
  newer = df5_renamed,
  boundary = as.Date("2014-10-12"),
  drift_days = 7L,
  # Stay_Book_In_Date — the date of initial intake for the stay — sharply
  # disambiguates two people booked into the same facility on the same day
  # with the same gender/citizenship.
  extra_match_cols = c("Stay_Book_In_Date"),
  # When n_o == n_n > 1 at the exact strict key, treat the group as the same
  # set of stints (anonymization differs between releases so individual
  # pairing is impossible) — drop older's rows without per-row field
  # coalesce. Safe: identical strict key guarantees same (bi, bo, fac,
  # gender, citizenship, stay_bi).
  block_merge_equal_n = TRUE,
  label = "df67<->df5 @ 2014-10-12"
)

df67_kept <- merge1$older
df5_after <- merge1$newer

rm(df67, df5_renamed, merge1); gc()

# --- Merge boundary 2: df5_after (older) <-> df4 (newer) at 2023-12-31 ---

# df5_after may contain rows with bi >= 2023-12-31 too (uwchr file FY2024 etc.)
# The helper drops older rows with bi >= boundary, consistent with the
# original "df5_trimmed < 2023-12-31" cutoff.

merge2 <- merge_boundary_crossers(
  older = df5_after,
  newer = df4_renamed,
  boundary = as.Date("2023-12-31"),
  drift_days = 7L,
  # df5 and df4 both carry Birth_Year and Stay_Book_In_Date — both add
  # disambiguation on top of (bi, bo, fac, gender, citizen).
  extra_match_cols = c("Birth_Year", "Stay_Book_In_Date"),
  block_merge_equal_n = TRUE,
  label = "df5<->df4 @ 2023-12-31"
)

df5_kept <- merge2$older
df4_kept <- merge2$newer

rm(df5_after, df4_renamed, merge2); gc()

# --- Combine into final dataset ---

detentions_df <- df67_kept |>
  safe_bind_rows(df5_kept) |>
  safe_bind_rows(df4_kept) |>
  janitor::clean_names(allow_dupes = FALSE) |>
  rename(
    most_serious_conviction_charge = msc_charge,
    most_serious_conviction_criminal_charge_category = most_serious_conviction_msc_criminal_charge_category,
    most_serious_conviction_conviction_date = msc_conviction_date,
    most_serious_conviction_sentence_days = msc_sentence_days,
    most_serious_conviction_sentence_months = msc_sentence_months,
    most_serious_conviction_sentence_years = msc_sentence_years,
    most_serious_conviction_crime_class = msc_crime_class
  ) |>
  select(where(is_not_blank_or_redacted)) |>
  redact_to_na() |>
  mutate(across(any_of("birth_year"), as.integer)) |>
  mutate(stay_ID = str_c(unique_identifier, "_", stay_book_in_date_time)) |>
  mutate(
    stint_ID = str_c(
      unique_identifier, "_", detention_book_in_date, "_", detention_facility_code
    )
  )

rm(df67_kept, df5_kept, df4_kept); gc()

detentions_df <- as.data.table(detentions_df)

order_cols <- intersect(
  c(
    "unique_identifier",
    "detention_book_in_date_and_time",
    "detention_facility_code",
    "detention_release_reason",
    "stay_book_out_date",
    "stay_release_reason",
    "religion",
    "gender",
    "marital_status",
    "birth_year",
    "ethnicity",
    "entry_status",
    "felon",
    "aggravated_felon_yes_no",
    "bond_posted_date",
    "bond_posted_amount",
    "case_status",
    "case_category",
    "final_order_yes_no",
    "final_order_date",
    "case_threat_level",
    "book_in_criminality",
    "final_charge",
    "departed_date",
    "departure_country",
    "citizenship_country",
    "final_program",
    "apprehension_final_program",
    "most_serious_conviction_charge_code",
    "most_serious_conviction_charge"
  ),
  names(detentions_df)
)
setorderv(detentions_df, order_cols)

detentions_df[,
  likely_duplicate := {
    if (all(is.na(unique_identifier))) {
      rep(NA, .N)
    } else if (all(is.na(initial_bond_set_amount))) {
      seq_len(.N) != .N
    } else {
      !seq_len(.N) %in%
        tail(
          which(
            initial_bond_set_amount ==
              min(initial_bond_set_amount, na.rm = TRUE)
          ),
          1
        )
    }
  },
  by = stint_ID
]

detentions_df |>
  as_tibble() |>
  col_vals_not_null(
    unique_identifier,
    actions = action_levels(warn_at = 0.70, stop_at = 0.85)
  ) |>
  col_vals_not_null(
    stint_ID,
    actions = action_levels(warn_at = 0.70, stop_at = 0.85)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.70, stop_at = 0.85)
  ) |>
  col_vals_between(
    any_of("birth_year"),
    1900L,
    as.integer(format(Sys.Date(), "%Y")),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    any_of("gender"),
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

save_historical_outputs(as_tibble(detentions_df), "detentions-historical")
