rm(list=ls())

library(dplyr)
library(lubridate)
library(vroom)
library(arrow)
library(readr)

# fast data reader
removals_data <- read_feather("./data/ice-processed/removals-merged.feather")|>
  mutate(Removal_Date = as.Date(Removal_Date))

weekly_counts <- removals_data |>
  filter(!is.na(Removal_Date)) |>
  mutate(
    week_start = floor_date(Removal_Date, unit = "week", week_start = 7), # Sunday
    year = year(week_start),
    week = week(week_start),
    year_week = sprintf("%d-W%02d", year, week)
  ) |>
  count(year_week, week_start, name = "n") |>
  arrange(week_start)

write_feather(weekly_counts, "data/ice-counts/removals-weekly-counts.feather")
