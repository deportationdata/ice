# ---- Packages ----
library(tidyverse)
library(tidylog)

# ---- Read in files ----

# Detention stints from March 2026 include MSC Charge and MSC Charge Code vars
url <- "https://github.com/deportationdata/ice/raw/bfc8b3fa7602fbf810fb811e6d07ef603e528b78/data/detention-stints-latest.parquet"
stints_mar_2026 <- arrow::read_parquet(url)

msc_charge_codes_tbl_prior <- stints_mar_2026 |>
  rename(msc_charge_code = most_serious_conviction_code) |>
  distinct(msc_charge, msc_charge_code) |>
  # Dropping one duplicated `msc_charge` value
  filter(!(msc_charge == "Identity Theft" & msc_charge_code == "70AA")) |>
  drop_na() |>
  arrange(msc_charge)

# Detainers from August 2026 include MSC Charge and MSC Charge Code vars
detainers_latest <- arrow::read_parquet(
  here::here("data/detainers-latest.parquet")
)

msc_charge_codes_tbl_latest <- detainers_latest |>
  rename(msc_charge = most_serious_conviction_charge) |>
  distinct(msc_charge, msc_charge_code) |>
  # Dropping one duplicated `msc_charge` value
  filter(!(msc_charge == "Identity Theft" & msc_charge_code == "70AA")) |>
  drop_na() |>
  arrange(msc_charge)

joined_charge_codes_tbl <- rbind(
  msc_charge_codes_tbl_prior,
  msc_charge_codes_tbl_latest
) |>
  distinct(msc_charge, msc_charge_code)

write_csv(msc_charge_codes_tbl, here::here("data/msc-charge-codes.csv"))

# END.