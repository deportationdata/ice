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


detentions_current <- arrow::read_feather("data/detentions-latest.feather")

detentions_2012_2023 <- arrow::read_feather(
  "data/ice-detentions-2012-2023.feather"
)

facilities_2017 <- arrow::read_feather("data/facilities-2017.feather")

facilities_foia_05655 <- arrow::read_feather(
  "data/facilities-foia-05655.feather"
)

name_code_match <-
  bind_rows(
    # "release_latest" = detentions_current |>
    #   distinct(detention_facility_code, detention_facility),
    # "release_aclu" = detentions_2012_2023 |>
    #   distinct(detention_facility_code, detention_facility),
    "2017" = facilities_2017 |>
      transmute(
        detention_facility_code = detloc,
        detention_facility = name,
        city,
        state
      ),
    "foia_05655" = facilities_foia_05655 |>
      distinct(
        detention_facility_code,
        detention_facility,
        city = detention_facility_city,
        state = detention_facility_state
      ),
    .id = "source"
  )

name_city_state_match <-
  name_code_match |>
  filter(!is.na(city), !is.na(state)) |>
  mutate(
    across(
      c(detention_facility, city, state),
      clean_text
    ),
    detention_facility = clean_facility_name(detention_facility)
  ) |>
  mutate(
    detention_facility_code = case_when(
      detention_facility == "ERIE COUNTY HOLDING CENTER" ~ "ERICONY",
      detention_facility == "FREDERICK HOLDROOM" ~ "FRDHOLD",
      detention_facility == "GRAYSON COUNTY DETENTION CENTER" ~ "GRAYSKY",
      detention_facility == "MONTGOMERY CITY JAIL" ~ "MNTGMAL",
      TRUE ~ detention_facility_code
    )
  ) |>
  slice_max(
    n = 1,
    by = c(detention_facility, city, state),
    order_by = source,
    with_ties = FALSE
  )

arrow::write_feather(
  name_city_state_match,
  "data/facilities-name-city-state-match.feather"
)

# # name_city_state_match |>
# #   mutate(n = n(), .by = c(detention_facility, city, state)) |>
# #   arrange(detention_facility_code, detention_facility, city, state, source) |>
# #   filter(n > 1)

# facilities_detention_management <- arrow::read_feather(
#   "data/facilities-detention-management.feather"
# )

# facilities_detention_management |>
#   mutate(
#     across(
#       c(name, city, state),
#       clean_text,
#       .names = "{.col}_merge"
#     ),
#     name_merge = clean_facility_name(name_merge)
#   ) |>
#   left_join(
#     name_city_state_match |> mutate(matched = 1),
#     by = c(
#       "name_merge" = "detention_facility",
#       "city_merge" = "city",
#       "state_merge" = "state"
#     )
#   ) |>
#   filter(is.na(matched)) |>
#   select(name:aor)

# facilities_scraped <- arrow::read_feather(
#   "data/facilities-from-ice-website.feather"
# )

# facilities_scraped |>
#   mutate(
#     across(
#       c(name, address, city, state),
#       clean_text,
#       .names = "{.col}_merge"
#     ),
#     name_merge = clean_facility_name(name_merge),
#     address_merge = clean_street_address(address_merge),
#   ) |>
#   anti_join(
#     name_city_state_match |> mutate(matched = 1),
#     by = c(
#       "name_merge" = "detention_facility",
#       "city_merge" = "city",
#       "state_merge" = "state"
#     )
#   ) |>
#   anti_join(
#     facilities_detention_management |>
#       mutate(
#         across(
#           c(address, city, state),
#           clean_text,
#           .names = "{.col}_dm"
#         ),
#         address_dm = clean_street_address(address_dm)
#       ) |>
#       select(address_dm, city_dm, state_dm),
#     by = c(
#       "address_merge" = "address_dm",
#       "city_merge" = "city_dm",
#       "state_merge" = "state_dm"
#     )
#   ) |>
#   inner_join(
#     facilities_foia_05655 |>
#       mutate(
#         across(
#           c(
#             detention_facility_address,
#             detention_facility_city,
#             detention_facility_state
#           ),
#           clean_text,
#           .names = "{.col}_05655"
#         ),
#         detention_facility_address_05655 = clean_street_address(
#           detention_facility_address_05655
#         )
#       ) |>
#       select(
#         detention_facility_address_05655,
#         detention_facility_city_05655,
#         detention_facility_state_05655
#       ),
#     by = c(
#       "address_merge" = "detention_facility_address_05655",
#       "city_merge" = "detention_facility_city_05655",
#       "state_merge" = "detention_facility_state_05655"
#     )
#   )

# facilities_scraped_addresses_1 <-
#   facilities_scraped |>
#   mutate(
#     across(
#       c(name, address, city, state),
#       clean_text,
#       .names = "{.col}_merge"
#     ),
#     name_merge = clean_facility_name(name_merge),
#     address_merge = clean_street_address(address_merge),
#   ) |>
#   left_join(
#     name_city_state_match |> mutate(matched = 1),
#     by = c(
#       "name_merge" = "detention_facility",
#       "city_merge" = "city",
#       "state_merge" = "state"
#     )
#   )

# facilities_scraped_addresses_1 |>
#   filter(is.na(matched)) |>
#   select(-matched) |>
#   left_join(
#     facilities_detention_management |>
#       mutate(
#         across(
#           c(address, city, state),
#           clean_text,
#           .names = "{.col}_dm"
#         ),
#         address_dm = clean_street_address(address_dm)
#       ) |>
#       select(name, address_dm, city_dm, state_dm) |>
#       mutate(matched = 1),
#     by = c(
#       "address_merge" = "name",
#       "city_merge" = "city_dm",
#       "state_merge" = "state_dm"
#     )
#   ) |>
#   select(name, address_merge, city_merge, state_merge) |>
#   arrange(state_merge, city_merge, address_merge)
