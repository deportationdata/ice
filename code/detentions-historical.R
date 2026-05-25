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
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")
source("code/functions/safe_bind_rows.R")
source("code/functions/save_historical_outputs.R")
source("code/functions/coalesce_rename.R")

col_type_overrides_march2026_detentions <- c(
  Stay_Book_In_Date_Time          = "date",
  Book_In_Date_Time               = "date",
  Detention_Facility              = "text",
  Book_Out_Date_Time              = "date",
  Stay_Book_Out_Date_Time         = "date",
  Detention_Release_Reason        = "text",
  Stay_Book_Out_Date              = "date",
  Stay_Release_Reason             = "text",
  Religion                        = "text",
  Gender                          = "text",
  Marital_Status                  = "text",
  Ethnicity                       = "text",
  Birth_Country                   = "text",
  Citizenship_Country             = "text",
  Entry_Status                    = "text",
  Known_Terrorist_Yes_No          = "text",
  Suspected_Gang_Yes_No           = "text",
  MSC_Charge                      = "text",
  MSC_Sentence_Days               = "numeric",
  MSC_Sentence_Months             = "numeric",
  MSC_Sentence_Years              = "numeric",
  MSC_Charge_Code                 = "text",
  Aggravated_Felon_Yes_No         = "text",
  Offense_INA_236C_Yes_No         = "text",
  Case_INA_236C_Yes_No            = "text",
  Bond_Posted_Date                = "date",
  Bond_Posted_Amount              = "numeric",
  Case_Status                     = "text",
  Case_Category                   = "text",
  Final_Order_Yes_No              = "text",
  Final_Order_Date                = "date",
  Case_Threat_Level               = "text",
  Detainee_Classification         = "text",
  Final_Charge                    = "text",
  Departed_Date                   = "date",
  Departure_Country               = "text",
  Initial_Bond_Set_Amount         = "numeric",
  Initial_Bond_Set_Date           = "date",
  Detention_Facility_Code         = "text",
  Birth_Date                      = "text",
  Birth_Year                      = "numeric",
  Book_In_Criminality             = "text",
  Race                            = "text",
  Entry_Date                      = "date",
  Apprehension_Final_Program      = "text",
  MSC_Charge_Date                 = "date",
  MSC_Conviction_Date             = "date",
  MSC_Criminal_Charge_Status      = "text",
  MSC_Criminal_Charge_Status_Code = "text",
  MSC_Crime_Class                 = "text",
  Book_In_Site                    = "text",
  Book_In_AOR                     = "text",
  Anonymized_Identifier           = "text"
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
      \(sh) process_sheet(
        file_path = fp,
        sheet = sh,
        anchor_idx = 2,
        guess_max = 10000,
        col_type_overrides = col_type_overrides_march2026_detentions
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
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))

col_type_overrides_uwchr_detentions <- c(
  Stay_Book_In_Date_Time                                = "date",
  Detention_Book_In_Date_And_Time                       = "date",
  Detention_Book_Out_Date_Time                          = "date",
  Stay_Book_Out_Date_Time                               = "date",
  Birth_Country_PER                                     = "text",
  Birth_Country_ERO                                     = "text",
  Citizenship_Country                                   = "text",
  Race                                                  = "text",
  Ethnic                                                = "text",
  Gender                                                = "text",
  Birth_Date                                            = "text",
  Birth_Year                                            = "numeric",
  Entry_Date                                            = "date",
  Entry_Status                                          = "text",
  Most_Serious_Conviction_MSC_Criminal_Charge_Category  = "text",
  MSC_Charge                                            = "text",
  MSC_Charge_Code                                       = "text",
  MSC_Conviction_Date                                   = "date",
  MSC_Sentence_Days                                     = "numeric",
  MSC_Sentence_Months                                   = "numeric",
  MSC_Sentence_Years                                    = "numeric",
  MSC_Crime_Class                                       = "text",
  Case_Threat_Level                                     = "text",
  Apprehension_Threat_Level                             = "text",
  Final_Program                                         = "text",
  Detention_Facility_Code                               = "text",
  Detention_Facility                                    = "text",
  Area_of_Responsibility                                = "text",
  Docket_Control_Office                                 = "text",
  Detention_Release_Reason                              = "text",
  Stay_Release_Reason                                   = "text",
  Alien_File_Number                                     = "text",
  Anonymized_Identifier                                 = "text"
)

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
        col_type_overrides = col_type_overrides_uwchr_detentions
      ),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))

col_type_overrides_emily_rif_detentions <- c(
  Citizenship_Country               = "text",
  Citizenship_Country_Code          = "text",
  Gender                            = "text",
  Gender_Code                       = "text",
  History_Intake_DCO                = "text",
  History_Detention_Facility        = "text",
  History_Detention_Facility_Code   = "text",
  Initial_Intake_Date               = "date",
  History_Intake_Date               = "date",
  History_Book_out_Date             = "date",
  History_Release_Reason            = "text",
  ERO_Apprehension_Date             = "date",
  ERO_Apprehension_Landmark         = "text",
  Initial_Intake_Detention_Facility = "text",
  Order_of_Detentions               = "text",
  Unique_Person_ID                  = "text",
  Unique_Detention_Stay_ID          = "text"
)

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
        col_type_overrides = col_type_overrides_emily_rif_detentions
      ),
      .id = "sheet_original"
    ),
    .id = "file_original"
  ) |>
  mutate(row_original = as.integer(row_number()), .by = c("file_original", "sheet_original")) |>
  select(where(is_not_blank_or_redacted)) |>
  mutate(across(where(~ inherits(.x, "POSIXt")), check_dttm_and_convert_to_date))

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
