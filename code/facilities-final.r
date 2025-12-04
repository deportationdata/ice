library(tidyverse)
library(tidylog)
library(arrow)
library(knitr)

# cleaning helper functions
clean_text <- function(str) {
  str |>
    str_trim() |>
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
    str_trim() |>
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
    str_trim() |>
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

# importing datasets
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
  dplyr::select(
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
  dplyr::select(
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
  dplyr::select(
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

# combine for each field one-by-one
facility_addresses <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated,
    "2017" = facilities_2017,
    "05655" = detentions_05655,
    "website" = facility_addresses_from_ice_website,
    "detention_management" = facilities_detention_management,
    .id = "source"
  )

# apply cleaning functions with NA handling
facility_addresses_clean <-
  facility_addresses |>
  mutate(
    across(
      c(
        name,
        address,
        city,
        state,
        aor,
        type,
        type_detailed,
        male_female,
        over_under_72_status,
        circuit,
        docket,
        field_office
      ),
      ~ ifelse(is.na(.x), NA_character_, clean_text(.x))
    ),

    across(
      c(name, name_merge),
      ~ ifelse(is.na(.x), NA_character_, clean_facility_name(.x))
    ),

    across(
      c(address),
      ~ ifelse(is.na(.x), NA_character_, clean_street_address(.x))
    ),

    across(
      c(zip, zip_4),
      ~ ifelse(is.na(.x), NA_character_, str_remove_all(.x, "[^0-9]"))
    ),

    zip = ifelse(
      # TODO: can you switch this to a case_when - easier to read when there are multiple cases like this
      is.na(zip),
      NA_character_,
      ifelse(
        nchar(zip) == 5,
        zip,
        ifelse(nchar(zip) > 5, substr(zip, 1, 5), str_pad(zip, 5, pad = "0")) # TODO: can you switch this to str_sub rather than substr?
      )
    ),

    zip_4 = ifelse(
      # same on case_when
      is.na(zip_4),
      NA_character_,
      ifelse(
        nchar(zip_4) == 4,
        zip_4,
        ifelse(
          nchar(zip_4) > 4,
          substr(zip_4, 1, 4),
          str_pad(zip_4, 4, pad = "0")
        )
      )
    ),

    across(
      any_of(c("city_merge", "state_merge")),
      ~ ifelse(is.na(.x), NA_character_, clean_text(.x)) # TODO: if_else
    )
  ) |>
  distinct(detention_facility_code, date, source, .keep_all = TRUE) # TODO: I believe there are no duplicates, so can we remove?

# analysis on all fields
all_fields <- c(
  "name",
  "address",
  "city",
  "state",
  "zip",
  "aor",
  "type",
  "type_detailed",
  "male_female",
  "over_under_72_status",
  "name_merge",
  "city_merge",
  "state_merge",
  "circuit",
  "docket",
  "ice_funded",
  "over_under_72",
  "field_office",
  "zip_4"
)

facility_pivot <-
  facility_addresses_clean |>
  dplyr::select(detention_facility_code, date, source, all_of(all_fields)) |> # TODO: no need for dplyr:: if loaded via tidyverse
  pivot_longer(
    cols = all_of(all_fields),
    names_to = "variable",
    values_to = "value"
  ) |>
  mutate(year = year(date)) |> # TODO: question in my mind is why do by year and not always pick the newest one
  filter(!is.na(value) & value != "") |>
  group_by(detention_facility_code, variable, year) |>
  summarize(
    value = first(value), # TODO: this will pick a random one I think because not sorted, is that what we want?
    date = first(date),
    source = first(source),
    .groups = "drop"
  ) |>
  arrange(variable, detention_facility_code, date)

pivot_changes_flagged <-
  facility_pivot |>
  group_by(variable, detention_facility_code) |>
  mutate(
    prev_value = lag(value),
    changed = if_else(is.na(prev_value), FALSE, value != prev_value)
  ) |>
  ungroup()

change_summary_all <-
  pivot_changes_flagged |>
  group_by(variable, detention_facility_code) |>
  summarize(
    n_obs = n(),
    n_changes = sum(changed, na.rm = TRUE),
    ever_changed = any(changed),
    .groups = "drop"
  )

variable_change_stats <-
  change_summary_all |>
  group_by(variable) |>
  summarize(
    total_facilities = n(),
    facilities_changed = sum(ever_changed),
    pct_changed = mean(ever_changed) * 100,
    .groups = "drop"
  ) |>
  arrange(desc(pct_changed))

facilities_with_changes_all <-
  change_summary_all |>
  filter(n_changes > 0)

pattern_results <-
  pivot_changes_flagged |>
  semi_join(
    facilities_with_changes_all,
    by = c("variable", "detention_facility_code")
  ) |>
  group_by(variable, detention_facility_code) |>
  summarize(
    pattern = paste(
      unique(paste0(value, " (", year, ")")),
      collapse = " → "
    ),
    values_over_time = paste(
      paste0(value, " (", year, ")"),
      collapse = " → "
    ),
    .groups = "drop"
  )

# helper functions for implementing best values
has_reversion <- function(values) {
  values <- values[!is.na(values)]
  if (length(values) < 3) {
    return(FALSE)
  }
  values[1] %in% values[-1] # original value reappears
}

# count number of reversions, specifically A → B → A
is_aba <- function(values) {
  values <- values[!is.na(values)]
  if (length(values) < 3) {
    return(FALSE)
  }
  values[1] == values[length(values)] && length(unique(values)) == 2
}

get_modes <- function(v) {
  v <- v[!is.na(v)]
  tb <- table(v)
  names(tb)[tb == max(tb)]
}

value_histories <-
  pivot_changes_flagged |>
  group_by(variable, detention_facility_code) |>
  arrange(date, .by_group = TRUE) |>
  summarize(
    history = list(paste0(value, " (", year, ", ", source, ")")),
    values = list(as.character(value)),
    years = list(year),
    sources = list(source),
    n_changes = sum(changed, na.rm = TRUE),
    .groups = "drop"
  )

# implementing best values
best_values <-
  value_histories |>
  rowwise() |>
  mutate(
    unique_values = list(unique(values)),
    n_unique = length(unique_values),
    modes = list(get_modes(values)),
    n_modes = length(modes),
    ends_with_original = values[length(values)] == values[1],
    has_reversion = has_reversion(values),
    is_aba = is_aba(values),

    best_value = case_when(
      # rule 1: never changes
      n_changes == 0 ~ values[1],

      # rules 2 and 3: changes but does not revert (keep final)
      !has_reversion ~ values[length(values)],

      # rule 4: A → B → A (use modal value)
      has_reversion & n_modes == 1 ~ modes[[1]],

      # rule 5: multiple modes or more than 5 changes
      TRUE ~ NA_character_
    ),

    # manual review flag
    review_flag = case_when(
      n_unique > 2 & n_modes > 1 ~ "multiple_modes",
      n_changes > 5 ~ "many_changes",
      has_reversion & n_modes > 1 ~ "reversion_multiple_modes",
      TRUE ~ NA_character_
    )
  ) |>
  ungroup()

# counts of A → B → A pattern
reversion_counts <-
  value_histories |>
  rowwise() |>
  mutate(
    is_aba = is_aba(values)
  ) |>
  ungroup() |>
  filter(is_aba) |>
  select(variable, detention_facility_code, values, years)

reversion_counts_summary <-
  reversion_counts |>
  group_by(variable) |>
  summarize(
    n_reversions = n(),
    .groups = "drop"
  )

# merging best_values with facilities data
best_values_wide <-
  best_values |>
  select(detention_facility_code, variable, best_value) |>
  pivot_wider(
    names_from = variable,
    values_from = best_value
  )

facility_list <-
  facility_addresses_clean |>
  distinct(detention_facility_code) # TODO: this chooses arbitrarily among duplicates, is that what we want?

facility_final <-
  facility_list |>
  left_join(best_values_wide, by = "detention_facility_code")

# metadata for diagnostics
best_values_metadata <-
  best_values |>
  select(
    detention_facility_code,
    variable,
    n_changes,
    n_unique,
    has_reversion,
    is_aba,
    review_flag
  ) |>
  pivot_wider(
    names_from = variable,
    values_from = c(n_changes, n_unique, has_reversion, is_aba, review_flag),
    names_glue = "{variable}_{.value}"
  )

facility_final_metadata <-
  facility_final |>
  left_join(best_values_metadata, by = "detention_facility_code")
