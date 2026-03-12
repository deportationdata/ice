rm(list=ls())

library(arrow)
library(dplyr)

rows_to_keep <- read_feather("code/detentions-cpang/detentions_rows_to_keep.feather")

detentions_clean <- read_feather("data/ice-processed/detentions-merged-with-flags.feather")|>
  mutate(row_id = row_number()) |>
  semi_join(rows_to_keep, by = "row_id")

detentions_clean <- detentions_clean |>
  select(-duplicate_likely, -drop_row, -row_id)

write_feather(detentions_clean, "data/ice-final/detentions-final-v2.feather")
