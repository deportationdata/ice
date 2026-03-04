rm(list=ls())

library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)

# read data
removals_counts <- read_feather("data/ice-counts/removals-weekly-counts.feather")|>
  rename(n_original = n)
removals_drop_dup <- read_feather("data/ice-counts/removals-weekly-counts-drop-duplicates.feather")|>
  rename(n_drop_duplicates = n)

removals_comparison <- merge(removals_counts, 
                            removals_drop_dup, 
                            by = c("year_week", "week_start"), 
                            all = TRUE)

removals_comparison <- removals_comparison |>
  mutate(n_drop = n_original - n_drop_duplicates)

ggplot(removals_comparison, aes(x = week_start, y = n_drop)) +
  geom_col(fill = "darkgreen") +
  theme_minimal()

ggplot(removals_comparison, aes(x = week_start, y = n_drop_duplicates)) +
  geom_col() +
  theme_minimal()

ggplot(removals_comparison, aes(x = week_start, y = n_original)) +
  geom_col() +
  theme_minimal()

