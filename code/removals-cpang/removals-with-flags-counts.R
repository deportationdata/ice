rm(list=ls())

library(dplyr)
library(lubridate)
library(arrow)

# fast data reader
removals_data <- read_feather("./data/ice-processed/removals-merged-with-flags.feather")|>
  mutate(Departed_Date = as.Date(Departed_Date))|>
  filter(drop_row == FALSE) # don't drop these rows

weekly_counts <- removals_data |>
  filter(!is.na(Departed_Date)) |>
  mutate(
    week_start = floor_date(Departed_Date, unit = "week", week_start = 7), # Sunday
    year = year(week_start),
    week = week(week_start),
    year_week = sprintf("%d-W%02d", year, week)
  ) |>
  group_by(year_week, week_start) |>
  summarise(
    n = n(),
    n_source_files = n_distinct(source_file),
    source_files = paste(sort(unique(source_file)), collapse = ", "),
    .groups = "drop"
  ) |>
  arrange(week_start)

write_feather(weekly_counts, "data/ice-counts/removals-weekly-counts-drop-duplicates.feather")
