# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Read in files ----

### This only works because March 2026 dataset is still latest:
df <- arrow::read_parquet(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.parquet"
)

msc_charge_codes_tbl <- df |>
  distinct(msc_charge, most_serious_conviction_code) |>
  # Dropping one duplicated `msc_charge` value
  filter(!(msc_charge == "Identity Theft" & most_serious_conviction_code == "70AA")) |>
  drop_na() |>
  arrange(msc_charge)

write_csv(msc_charge_codes_tbl, here::here("data/msc-charge-codes.csv"))

# END.