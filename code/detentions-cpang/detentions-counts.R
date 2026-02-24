rm(list=ls())

library(dplyr)
library(tidyr)
library(lubridate)
library(arrow)

detentions_data <- read_feather(
  "data/ice-processed/detentions-merged.feather",
  col_select = c("Detention_Book_In_Date", "Stay_Book_In_Date")   # read only what you need
)

weekly_counts_bookIn <- detentions_data |>
  filter(!is.na(Detention_Book_In_Date)) |>
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date),
    week_start = floor_date(Detention_Book_In_Date, unit = "week", week_start = 7)
  ) |>
  count(week_start, name = "n") |>
  arrange(week_start)

write_feather(weekly_counts_bookIn, "data/ice-counts/detentions-weekly-counts.feather")
