rm(list = ls()); gc()

library(arrow)
library(dplyr)
library(data.table)

# 1) Read full dataset
detentions_data <- read_feather(
  "data/ice-processed/detentions-merged.feather"
) %>%
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date),
    row_id = row_number()
  )

# 2) Create SKINNY table using dplyr
skinny <- detentions_data %>%
  select(row_id, Unique_Identifier, Detention_Book_In_Date) %>%
  as.data.table()

# Create these integer versions for faster computation
skinny[, Detention_Book_In_Date := as.IDate(Detention_Book_In_Date)]
skinny[, uid := .GRP, by = Unique_Identifier]

# 3) Compute flags on skinny table only
setorder(skinny, uid, Detention_Book_In_Date)

# One lag-based indicator: is this row within 1 day of the previous row in the same uid?
skinny[, gap_le1 := (Detention_Book_In_Date - shift(Detention_Book_In_Date, type = "lag")) <= 1, by = uid]
skinny[is.na(gap_le1), gap_le1 := FALSE]

# "Within 1 day of next" is just lead(gap_le1)
skinny[, gap_le1_next := shift(gap_le1, type = "lead"), by = uid]
skinny[is.na(gap_le1_next), gap_le1_next := FALSE]

# Final flags (same intent as before)
skinny[, `:=`(
  duplicate_likely = !is.na(Unique_Identifier) & (gap_le1 | gap_le1_next),
  drop_row         = !is.na(Unique_Identifier) & gap_le1
)]

# Clean up intermediates to save memory
skinny[, c("gap_le1", "gap_le1_next", "uid") := NULL]

# Restore original order and bind back without a join
setorder(skinny, row_id)
detentions_data <- detentions_data %>%
  mutate(
    duplicate_likely = skinny$duplicate_likely,
    drop_row         = skinny$drop_row
  ) %>%
  select(-row_id)

write_feather(detentions_data, "data/ice-processed/detentions-merged-with-flags.feather")
