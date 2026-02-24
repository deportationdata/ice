rm(list=ls())

library(dplyr)
library(lubridate)
library(vroom)
library(arrow)

# fast data reader
detainers_data <- vroom("./data/ice-processed/detainers-merged.csv",
    col_types = cols(.default = col_character()),
  na = c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN"))
  
detainers_data2 <- detainers_data |>
  mutate(Prepare_Date = as.Date(Prepare_Date))

weekly_counts <- detainers_data2 |>
  filter(!is.na(Prepare_Date)) |>
  mutate(
    week_start = floor_date(Prepare_Date, unit = "week", week_start = 7), # Sunday
    year = year(week_start),
    week = week(week_start),
    year_week = sprintf("%d-W%02d", year, week)
  ) |>
  count(year_week, week_start, name = "n") |>
  arrange(week_start)

weekly_counts |>
  write_feather("data/ice-counts/detainers-weekly-counts.feather")
