rm(list=ls())

library(dplyr)
library(tidyverse)

detentions <- arrow::read_feather("data/ice-processed/detentions-merged-with-flags.feather", 
col_select = c("Detention_Book_In_Date", "Detention_Facility", "Detention_Facility_Code", "Detention_Book_Out_Date", "Gender", "Unique_Identifier", "drop_row"))

detentions <- detentions |>
  filter(drop_row == TRUE)|>
  mutate(
    Detention_Book_Out_Date = as.Date(Detention_Book_Out_Date)
    )

gender_summary <- detentions |>
  group_by(Detention_Facility, Detention_Facility_Code)|>
  summarize(first_used = as.Date(min(
      pmin(Detention_Book_Out_Date, Detention_Book_In_Date, na.rm = TRUE),
      na.rm = TRUE
    )),
    last_used = as.Date(max(
      pmax(Detention_Book_Out_Date, Detention_Book_In_Date, na.rm = TRUE),
      na.rm = TRUE
    )),
    n_stints = n(),
    n_stints_2025 = sum(Detention_Book_In_Date >= "2025-01-01", na.rm = TRUE),
    n_individuals = n_distinct(Unique_Identifier),
    n_individuals_2025 = sum(
      Detention_Book_In_Date >= "2025-01-01",
      na.rm = TRUE
    ),
    proportion_male = mean(
      Gender == "Male" & Gender != "Unknown",
      na.rm = TRUE
    ),
    proportion_female = mean(
      Gender == "Female" & Gender != "Unknown", 
      na.rm=TRUE
    ),
    proportion_unknown = mean(
      Gender == "Unknown", 
      na.rm=TRUE
    ),
    .groups = "drop"
  )

gender_summary_2025 <- gender_summary |>
  filter(n_stints_2025 > 0)|>
  mutate(
    facility_gender = case_when(
      proportion_male >= 0.95 ~ "Male", 
      proportion_female >= 0.95 ~ "Female", 
      TRUE ~ "both"
    )
  )

write_feather(gender_summary_2025, "data/detention-stints-gender-post-2025.feather")
