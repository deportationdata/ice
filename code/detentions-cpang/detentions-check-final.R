rm(list=ls())

library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)
library(lubridate)

detentions_temp <- read_feather(here::here("data/ice-final/detentions-final-v2.feather"), col_select = c("Detention_Book_In_Date", "source_file"))|>
  mutate(
    week_start = floor_date(Detention_Book_In_Date, unit = "week", week_start = 7)
  )|>
    arrange(week_start)

weekly_counts <- detentions_temp |>
  group_by(source_file, week_start) |>
  summarise(
    n = n(),
    .groups = "drop"
  ) |>
  arrange(source_file, week_start)

ggplot(weekly_counts, 
       aes(x = week_start, y = n, color = source_file)) +
  geom_line(size = 0.5, alpha = 0.5) +
  labs(
    title = "Weekly Detentions Counts by Source File",
    x = "Week Start",
    y = "Number of Detentions",
    color = "Source File"
  ) +
  theme_minimal()
