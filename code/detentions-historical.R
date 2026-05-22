# Source-file selection
# | df  | folder                      | window                   | role    | reason                                |
# |-----|-----------------------------|--------------------------|---------|---------------------------------------|
# | df6 | From-Emily-Excel-X-RIF      | 2010-05-30 .. 2014-10-11 | use     | only pre-2014 coverage                |
# | df5 | uwchr                       | 2014-10-12 .. 2023-12-30 | use     | mid-window coverage                   |
# | df4 | March 2026 Release          | >= 2023-12-31            | use     | most recent release                   |
# | df7 | From-Emily-FOIA-10-2554-527 | (whole)                  | use     | merged with df6 to fill gaps          |
# | df1 | 2019-ICFO-21307             | (whole)                  | exclude | not used downstream                   |
# | df2 | 2023_ICFO_42034             | (whole)                  | exclude | superseded by March 2026 Release      |
# | df3 | 2024-ICFO-41855             | (whole)                  | exclude | superseded by March 2026 Release      |

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
library(data.table)
library(lubridate)
library(tidylog)
library(pointblank)
library(haven)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")
source("code/functions/convert_temporal_columns.R")
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/safe_bind_rows.R")
source("code/functions/save_historical_outputs.R")
source("code/functions/coalesce_rename.R")

col_types_march2026_detentions <- c(
  "date",    # Stay Book In Date Time
  "date",    # Book In Date Time
  "text",    # Detention Facility
  "date",    # Book Out Date Time
  "date",    # Stay Book Out Date Time
  "text",    # Detention Release Reason
  "date",    # Stay Book Out Date
  "text",    # Stay Release Reason
  "text",    # Religion
  "text",    # Gender
  "text",    # Marital Status
  "text",    # Ethnicity
  "text",    # Birth Country
  "text",    # Citizenship Country
  "text",    # Entry Status
  "text",    # Known Terrorist Yes No
  "text",    # Suspected Gang Yes No
  "text",    # MSC Charge
  "numeric", # MSC Sentence Days
  "numeric", # MSC Sentence Months
  "numeric", # MSC Sentence Years
  "text",    # MSC Charge Code
  "text",    # Aggravated Felon Yes No
  "text",    # Offense INA 236C Yes No
  "text",    # Case INA 236C Yes No
  "date",    # Bond Posted Date
  "numeric", # Bond Posted Amount
  "text",    # Case Status
  "text",    # Case Category
  "text",    # Final Order Yes No
  "date",    # Final Order Date
  "text",    # Case Threat Level
  "text",    # Detainee Classification
  "text",    # Final Charge
  "date",    # Departed Date
  "text",    # Departure Country
  "numeric", # Initial Bond Set Amount
  "date",    # Initial Bond Set Date
  "text",    # Detention Facility Code
  "text",    # Birth Date
  "numeric", # Birth Year
  "text",    # Book In Criminality
  "text",    # Race
  "date",    # Entry Date
  "text",    # Apprehension Final Program
  "date",    # MSC Charge Date
  "date",    # MSC Conviction Date
  "text",    # MSC Criminal Charge Status
  "text",    # MSC Criminal Charge Status Code
  "text",    # MSC Crime Class
  "text",    # Book In Site
  "text",    # Book In AOR
  "text"     # Anonymized Identifier
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
      \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000, force_col_type = "text"),
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
      \(sh) process_sheet(file_path = fp, sheet = sh, anchor_idx = 2, guess_max = 10000, force_col_type = "text"),
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

df6_trimmed <- df6 |>
  filter(History_Intake_Date > as.Date("2010-05-23") + 7 & History_Intake_Date < as.Date("2014-10-12"))
df5_trimmed <- df5 |>
  filter(Detention_Book_In_Date_And_Time >= as.Date("2014-10-12") & Detention_Book_In_Date_And_Time < as.Date("2023-12-31"))
df4_trimmed <- df4 |>
  filter(Book_In_Date_Time >= as.Date("2023-12-31"))

df67 <- df6_trimmed |>
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
  safe_bind_rows(
    df7 |> rename(
      Detention_Book_In_Date = Book_In_Date,
      Detention_Book_Out_Date = Book_Out_Date,
      Book_In_DCO = Book_In_Dco
    )
  )

df45 <- df4_trimmed |>
  coalesce_rename("Detention_Book_In_Date_And_Time", "Book_In_Date_Time") |>
  coalesce_rename("Most_Serious_Conviction_Charge_Code", "Most_Serious_Conviction_MSC_Charge_Code") |>
  coalesce_rename("Most_Serious_Conviction_Charge_Code", "MSC_Charge_Code") |>
  safe_bind_rows(
    df5_trimmed |>
      coalesce_rename("Ethnicity", "Ethnic") |>
      coalesce_rename("Most_Serious_Conviction_Charge_Code", "MSC_Charge_Code") |>
      coalesce_rename("Unique_Identifier", "Anonymized_Identifier")
  ) |>
  mutate(Detention_Book_In_Date = as.Date(Detention_Book_In_Date_And_Time),
         Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date_Time),
         Birth_Country = coalesce(Birth_Country_ERO, Birth_Country_PER)) |>
  select(-Birth_Country_ERO, -Birth_Country_PER)

rm(df4, df5, df6, df7, df4_trimmed, df5_trimmed, df6_trimmed)
gc()

detentions_df <- df67 |>
  rename(Detention_Release_Reason = Release_Reason) |>
  safe_bind_rows(
    df45 |> rename(Book_In_DCO = Docket_Control_Office)
  ) |>
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
      unique_identifier,
      "_",
      detention_book_in_date,
      "_",
      detention_facility_code
    )
  )

rm(df45, df67)
gc()

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
    actions = action_levels(warn_at = 0.30, stop_at = 0.50)
  ) |>
  col_vals_not_null(
    stint_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
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
