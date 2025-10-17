library(tidyverse)
library(tidylog)

clean_text <- function(str) {
  str |>
    str_to_lower() |>
    str_replace_all(
      "(?<=\\b)([A-Za-z]\\.)+(?=\\b)",
      ~ str_remove_all(.x, "\\.")
    ) |>
    str_replace_all("[[:punct:]]", " ") |>
    str_squish()
}

clean_facility_name <- function(str) {
  str |>
    str_replace_all(c(
      "\\bcty\\b" = "county",
      "\\bdept of corr\\b" = "department of corrections",
      "\\bcnt\\b" = "center",
      "\\busp\\b" = "us penitentiary",
      "\\bdet\\b" = "detention",
      "\\bfed\\b" = "federal",
      "\\bmet\\b" = "metropolitan",
      "\\bjuv\\b" = "juvenile",
      "\\bctr\\b" = "center",
      "\\bco\\b" = "county",
      "\\bfac\\b" = "facility",
      "\\bproc\\b" = "processing",
      "\\bpen\\b" = "penitentiary",
      "\\bcor\\b" = "correctional",
      "\\bcorr\\b" = "correctional",
      "\\bfacilty\\b" = "facility",
      "\\bfclty\\b" = "facility",
      "\\bspc\\b" = "service processing center",
      "\\breg\\b" = "regional",
      "\\bdept\\b" = "department",
      "\\bdf\\b" = "detention facility",
      "\\bpub\\b" = "public",
      "\\bsfty\\b" = "safety",
      "\\bcplx\\b" = "complex",
      "\\bcorrec\\b" = "correctional",
      "\\bcdf\\b" = "contract detention facility",
      "\\brm\\b" = "room",
      "\\bfo\\b" = "field office"
    ))
}


clean_street_address <- function(str) {
  str |>
    str_replace_all(c(
      "\\bstreet\\b" = "st",
      "\\bavenue\\b" = "ave",
      "\\bboulevard\\b" = "blvd",
      "\\bdrive\\b" = "dr",
      "\\broad\\b" = "rd",
      "\\bhighway\\b" = "hwy",
      "\\bplace\\b" = "pl",
      "\\blane\\b" = "ln",
      "\\bsquare\\b" = "sq",
      "\\bterrace\\b" = "ter",
      "\\bparkway\\b" = "pkwy",
      "\\bcourt\\b" = "ct",
      "\\bnorth\\b" = "n",
      "\\bsouth\\b" = "s",
      "\\beast\\b" = "e",
      "\\bwest\\b" = "w",
      "\\bfort\\b" = "ft"
    ))
}

name_city_state_match <- arrow::read_feather(
  "data/facilities-name-city-state-match.feather"
)

facilities_2017 <-
  arrow::read_feather("data/facilities-2017.feather") |>
  transmute(
    detention_facility_code = detloc,
    name,
    address,
    city,
    state,
    zip = as.character(zip),
    circuit,
    aor,
    docket,
    type,
    type_detailed,
    ice_funded,
    male_female
  ) |>
  mutate(date = as.Date("2017-11-06")) # date in file

facilities_dedicated_nondedicated <-
  arrow::read_feather("data/facilities-dedicated-nondedicated.feather") |>
  select(
    name,
    address,
    city,
    state,
    zip,
    aor,
    type,
    type_detailed,
    male_female,
    over_under_72_status,
    date = file_original_date
  ) |>
  mutate(
    across(
      c(name, city, state),
      clean_text,
      .names = "{.col}_merge"
    ),
    name_merge = clean_facility_name(name_merge)
  ) |>
  slice_max(
    order_by = date,
    n = 1,
    with_ties = FALSE,
    by = c("name_merge", "city_merge", "state_merge")
  ) |>
  left_join(
    name_city_state_match,
    by = c(
      "name_merge" = "detention_facility",
      "city_merge" = "city",
      "state_merge" = "state"
    )
  ) |>
  filter(!is.na(detention_facility_code))


detentions_05655 <-
  arrow::read_feather(
    "data/facilities-foia-05655.feather"
  ) |>
  select(
    detention_facility_code,
    name = detention_facility,
    address = detention_facility_address,
    city = detention_facility_city,
    state = detention_facility_state,
    zip = detention_facility_zip_code,
    type = detention_facility_type,
    type_detailed = detention_facility_type_detailed,
    ice_funded = detention_facility_ice_funded,
    male_female = detention_facility_male_female,
    over_under_72 = detention_facility_over_under_72
  ) |>
  mutate(date = as.Date("2024-01-01")) # approximate date of FOIA request; data ends in Oct. 2023 but dated 2024

facility_addresses_from_ice_website <-
  arrow::read_feather(
    "data/facilities-from-ice-website.feather"
  ) |>
  mutate(date = as.Date("2025-10-01")) |> # approximate date of website scrape
  mutate(
    across(
      c(name, city, state),
      clean_text,
      .names = "{.col}_merge"
    ),
    name_merge = clean_facility_name(name_merge)
  ) |>
  left_join(
    name_city_state_match,
    by = c(
      "name_merge" = "detention_facility",
      "city_merge" = "city",
      "state_merge" = "state"
    )
  ) |>
  filter(!is.na(detention_facility_code))

facilities_detention_management <-
  arrow::read_feather(
    "data/facilities-detention-management.feather"
  ) |>
  select(
    name,
    address,
    city,
    state,
    zip,
    date,
    aor,
    type_detailed,
    male_female
  ) |>
  mutate(
    across(
      c(name, city, state),
      clean_text,
      .names = "{.col}_merge"
    ),
    name_merge = clean_facility_name(name_merge)
  ) |>
  left_join(
    name_city_state_match,
    by = c(
      "name_merge" = "detention_facility",
      "city_merge" = "city",
      "state_merge" = "state"
    )
  ) |>
  filter(!is.na(detention_facility_code))

# now combine for each field one-by-one

facility_addresses <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    "website" = facility_addresses_from_ice_website,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  filter(!is.na(address) & address != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    address,
    city,
    state,
    zip,
    source,
    date
  )

facility_type <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    .id = "source"
  ) |>
  filter(!is.na(type) & type != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    type,
    source,
    date
  )

facility_type_detailed <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  filter(!is.na(type_detailed) & type_detailed != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    type_detailed,
    source,
    date
  )

facility_ice_funded <-
  bind_rows(
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    .id = "source"
  ) |>
  filter(!is.na(ice_funded) & ice_funded != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    ice_funded,
    source,
    date
  )

facility_male_female <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  filter(!is.na(male_female) & male_female != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    male_female,
    source,
    date
  )

facility_over_under_72 <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "05655" = detentions_05655,
    .id = "source"
  ) |>
  filter(!is.na(over_under_72) & over_under_72 != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    over_under_72,
    source,
    date
  )

facility_addresses <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    "website" = facility_addresses_from_ice_website,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  filter(!is.na(address) & address != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    address,
    city,
    state,
    zip,
    source,
    date
  )

facility_aor <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  filter(!is.na(aor) & aor != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    aor,
    source,
    date
  )

facility_docket <-
  bind_rows(
    "2017" = facilities_2017,
    .id = "source"
  ) |>
  filter(!is.na(docket) & docket != "") |>
  select(
    detention_facility_code,
    docket,
    source,
    date
  )

facility_names <-
  bind_rows(
    "2017" = facilities_2017,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  filter(!is.na(name) & name != "") |>
  slice_max(
    n = 1,
    order_by = date,
    by = detention_facility_code,
    with_ties = FALSE
  ) |>
  select(
    detention_facility_code,
    name,
    source,
    date
  )

facility_list <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    "website" = facility_addresses_from_ice_website,
    "detention_management" = facilities_detention_management,
    .id = "source"
  ) |>
  distinct(detention_facility_code)

facility_df <-
  facility_list |>
  left_join(
    facility_names,
    by = "detention_facility_code"
  ) |>
  rename(
    name_source = source,
    name_date = date
  ) |>
  left_join(
    facility_addresses,
    by = "detention_facility_code",
    suffix = c("", "_address")
  ) |>
  left_join(
    facility_type,
    by = "detention_facility_code",
    suffix = c("", "_type")
  ) |>
  left_join(
    facility_type_detailed,
    by = "detention_facility_code",
    suffix = c("", "_type_detailed")
  ) |>
  left_join(
    facility_ice_funded,
    by = "detention_facility_code",
    suffix = c("", "_ice_funded")
  ) |>
  left_join(
    facility_male_female,
    by = "detention_facility_code",
    suffix = c("", "_male_female")
  ) |>
  left_join(
    facility_over_under_72,
    by = "detention_facility_code",
    suffix = c("", "_over_under_72")
  ) |>
  left_join(
    facility_aor,
    by = "detention_facility_code",
    suffix = c("", "_aor")
  ) |>
  left_join(
    facility_docket,
    by = "detention_facility_code",
    suffix = c("", "_docket")
  ) |>
  # rename so suffixes become prefixes
  rename_with(
    ~ str_replace(.x, "^(source|date)_(.*)$", "\\2_\\1"),
    matches("^(source|date)_")
  )

arrow::write_feather(
  facility_df,
  "data/facilities-details.feather"
)
