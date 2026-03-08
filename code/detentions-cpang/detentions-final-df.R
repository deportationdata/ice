rm(list=ls())

library(arrow)
library(dplyr)

rows_to_drop <- read_feather("code/detentions-cpang/detentions_rows_to_drop.feather")

detentions_clean <- read_feather("data/ice-processed/detentions-merged-with-flags.feather")|>
  mutate(row_id = row_number())|>
  anti_join(rows_to_drop, by = "row_id")

detentions_clean <- detentions_clean |>
  select(-duplicate_likely, -drop_row, -row_id)

write_feather(detentions_clean, "data/ice-final/detentions-final.feather")