rm(list=ls())

library(dplyr)
library(tidyr)
library(lubridate)
library(vroom)
library(arrow)

detentions_data <- read_feather("data/ice-processed/detentions-merged.feather")|>
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date)
  )

weekly_counts_bookIn <- detentions_data |>
  filter(!is.na(Detention_Book_In_Date)) |>
  mutate(
    week_start = floor_date(Detention_Book_In_Date, unit = "week", week_start = 7), # Sunday
    year = year(week_start),
    week = week(week_start),
    year_week = sprintf("%d-W%02d", year, week)
  ) |>
  count(year_week, week_start, name = "n") |>
  arrange(week_start)
