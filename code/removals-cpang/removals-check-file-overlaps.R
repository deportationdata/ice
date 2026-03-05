rm(list=ls())

library(dplyr)
library(lubridate)
library(arrow)
library(ggplot2)

# fast data reader
removals_data <- read_feather("./data/ice-processed/removals-merged-with-flags.feather")|>
  mutate(Departed_Date = as.Date(Departed_Date))|>
  filter(drop_row == FALSE)|>
  mutate(
    # for < 2013-09-29 14-03290 dominates, afterwards 2023_ICFO_42034 takes over
    keep_row = case_when(
      source_file == "14-03290" & Departed_Date < as.Date("2013-09-29") ~ TRUE, 
      source_file == "2023_ICFO_42034" & Departed_Date >= as.Date("2013-09-29") & Departed_Date < as.Date("2023-10-01") ~ TRUE, 
      source_file == "82025" & Departed_Date >= as.Date("2023-10-01") ~ TRUE, 
      TRUE ~ FALSE
    )
  )|>
  filter(keep_row == TRUE)

weekly_counts <- removals_data |>
  filter(!is.na(Departed_Date)) |>
  mutate(
    week_start = floor_date(Departed_Date, unit = "week", week_start = 7), # Sunday
    year = year(week_start),
    week = week(week_start),
    year_week = sprintf("%d-W%02d", year, week)
  ) |>
  arrange(week_start)

weekly_counts_by_src <- weekly_counts |>
  group_by(source_file, year_week, week_start) |>
  summarise(
    n = n(),
    .groups = "drop"
  ) |>
  arrange(source_file, week_start)

ggplot(weekly_counts_by_src, 
       aes(x = week_start, y = n, color = source_file)) +
  geom_line(size = 0.5, alpha = 0.9) +
  labs(
    title = "Weekly Removal Counts by Source File",
    x = "Week Start",
    y = "Number of Removals",
    color = "Source File"
  ) +
  theme_minimal()


removals_out <- removals_data |>
  select(-duplicate_likely, -drop_row, -keep_row)

write_feather(weekly_counts_by_src, "data/ice-counts/removals-weekly-counts-final.feather")
write_feather(removals_out, "data/ice-final/removals-final.feather")

