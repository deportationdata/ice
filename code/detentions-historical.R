# Source-file selection
# | df  | folder                      | window                   | role | reason                              |
# |-----|-----------------------------|--------------------------|------|-------------------------------------|
# | df6 | From-Emily-Excel-X-RIF      | 2010-05-30 .. 2014-10-11 | use  | only pre-2014 coverage              |
# | df5 | uwchr                       | 2014-10-12 .. 2023-12-30 | use  | mid-window coverage                 |
# | df4 | 120125                      | >= 2023-12-31            | use  | most recent release                 |
# | df7 | From-Emily-FOIA-10-2554-527 | (whole)                  | use  | merged with df6 to fill gaps        |

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(data.table)
library(arrow)
library(ggplot2)
library(lubridate)
library(tidylog)

# --- Source Functions ---
source("code/functions/safe_bind_rows.R")
# source("code/functions/inspect_columns.R")
# source("code/functions/summarize_weekly_counts.R")

df4 <- read_parquet("data/cache/detentions-120125.parquet")|> as_tibble()
df5 <- read_parquet("data/cache/detentions-uwchr.parquet")|> as_tibble()
df6 <- read_parquet("data/cache/detentions-From-Emily-Excel-X-RIF.parquet")|> as_tibble()
df7 <- read_parquet("data/cache/detentions-From-Emily-FOIA-10-2554-527.parquet")|> as_tibble()

# filter dates that we want 
df6_trimmed <- df6 |>
  filter(History_Intake_Date > as.Date("2010-05-23")+7 & History_Intake_Date < as.Date("2014-10-12"))
df5_trimmed <- df5 |>
  filter(Detention_Book_In_Date_And_Time >= as.Date("2014-10-12") & Detention_Book_In_Date_And_Time < as.Date("2023-12-31"))
df4_trimmed <- df4 |> 
  filter(Book_In_Date_Time >= as.Date("2023-12-31"))

# merge dataframes!
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
  rename(
    Detention_Book_In_Date_And_Time = Book_In_Date_Time,
    Most_Serious_Conviction_Charge_Code = Most_Serious_Conviction_MSC_Charge_Code
  ) |>
  safe_bind_rows(
    df5_trimmed |> rename(
      Ethnicity = Ethnic,
      Most_Serious_Conviction_Charge_Code = MSC_Charge_Code,
      Unique_Identifier = Anonymized_Identifier
    )
  ) |>
  mutate(Detention_Book_In_Date = as.Date(Detention_Book_In_Date_And_Time),
         Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date_Time),
         Birth_Country = coalesce(Birth_Country_ERO, Birth_Country_PER)) |>
  select(-Birth_Country_ERO, -Birth_Country_PER)

rm(list = setdiff(ls(), c("df67", "df45")))
gc()

df_all <- df67 |>
  rename(Detention_Release_Reason = Release_Reason) |>
  safe_bind_rows(
    df45 |> rename(Book_In_DCO = Docket_Control_Office)
  )

# # check final dataframe
# all_weekly_counts <- get_weekly_counts(df_all, "Detention_Book_In_Date")

# ggplot(all_weekly_counts ,aes(week_start, y=n, color = source_file))+
#   geom_line(alpha = 0.5)+
#   theme_minimal()

write_parquet(df_all, "data/cache/detentions-historical-no-flags.parquet", compression = "zstd", compression_level = 19)
