rm(list=ls())

library(dplyr)
library(lubridate)
library(arrow)

# fast data reader
arrests_data <- read_feather("data/ice-processed/arrests-merged.feather")
  
arrests_data2 <- arrests_data |>
  mutate(Apprehension_Date = as.Date(Apprehension_Date))

weekly_counts <- arrests_data2 |>
  filter(!is.na(Apprehension_Date)) |>
  mutate(
    week_start = floor_date(Apprehension_Date, unit = "week", week_start = 7), # Sunday
    year = year(week_start),
    week = week(week_start),
    year_week = sprintf("%d-W%02d", year, week)
  ) |>
  count(year_week, week_start, name = "n") |>
  arrange(week_start)

weekly_counts |>
  write_feather("data/ice-counts/arrests-weekly-counts.feather")
