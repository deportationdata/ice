rm(list=ls())

library(dplyr)
library(arrow)

# read data
arrests_counts <- read_feather("data/ice-counts/arrests-weekly-counts.feather")
detainers_counts <- read_feather("data/ice-counts/detainers-weekly-counts.feather")
detentions_counts <- read_feather("data/ice-counts/detentions-weekly-counts.feather")
encounters_counts <- read_feather("data/ice-counts/encounters-weekly-counts.feather")
removals_counts <- read_feather("data/ice-counts/removals-weekly-counts.feather")

library(purrr)

tables <- list(
  detentions = detentions_counts,
  arrests    = arrests_counts,
  detainers  = detainers_counts, 
  encounters = encounters_counts, 
  removals = removals_counts
)

tables2 <- imap(tables, \(df, nm) df |> rename(!!paste0("n_", nm) := n))

weekly_all <- reduce(tables2, full_join, by = "week_start") |>
  arrange(week_start) |>
  mutate(across(starts_with("n_"), ~ tidyr::replace_na(.x, 0L)))

weekly_long <- weekly_all |>
  pivot_longer(
    cols = starts_with("n_"),
    names_to = "event_type",
    values_to = "count"
  )|>
  mutate(
    event_type = gsub("^n_", "", event_type)
  )


write_feather(weekly_long, "data/ice-counts/all-events-by-week.feather")
