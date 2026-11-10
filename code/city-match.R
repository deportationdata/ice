# This script matches normalized city/state key values generated in `arrests-latest-read.R`
# with unique geographic subdivisions (cells) for the entire U.S. and territories generated in `us-shapefiles` repository.
#
# Matches are performed in five tiers, plus a preliminary step which excludes bad values and final unmatched tier (T0, T1, ..., T5, T6).
# After each match tier, the remainder of unmatched records is passed through to the next tier.
# All tiers are concatenated for export as `city-match-crosswalk.parquet`.
# Records not yet assigned to counties are joined with counties in `city-county.R`.
#
# Tiers:
# T0: Excludes garbage values, records missing state key, and some known non-place values defined in `stoplist`
# T1: Manual match for specific known values (neighborhoods, obvious data entry errors)
# T2: Exact matches with a named place
# T3: Exact matches with a county subdivision
# T4: Exact matches with a county
# T5: Remaining city/state keys matched to GNIS places corresponding to single points within a cell
# T6: Unmatched remainder

library(tidyverse)

source("code/functions/normalize_city.R") # Helper functions/objects for normalizing city/state names
shp_url <- "https://github.com/deportationdata/us-shapefiles/raw/653d6cc5fb6fd73bc682e11e6b60a3ef0c4d8e0a/data"
options(timeout = 1800) # the 17 MB counties file downloads at ~200 KB/s from GitHub LFS

# ==== References ==============================================================

gnis_review_min <- 50
manual_review_min <- 20 # Minimum number of observations for unmatched review export

crosswalk_in_all <-
  arrow::read_parquet(
    str_c(shp_url, "/places-county-subdivisions-xwalk.parquet")
  ) |>
  left_join(state_names, by = "stusps")

# Exclude a small proportion of county subdivisions that can't be mapped to a single cell
crosswalk_in <- crosswalk_in_all |> filter(!is.na(cell_geoid))

# Separate out ambiguous census places; brought back after GNIS match in T5
declined_places <-
  crosswalk_in_all |>
  filter(geo_level == "place") |>
  mutate(key_city = normalize_city(name)) |>
  filter(n_distinct(geoid) > 1, .by = c(key_city, key_state)) |>
  distinct(key_city, key_state) |>
  mutate(place_note = "ambiguous census place")

places_lookup <-
  crosswalk_in |>
  filter(geo_level == "place") |>
  transmute(
    key_city = normalize_city(name),
    key_state,
    stusps,
    raw_geoid = geoid,
    raw_name = namelsad,
    raw_level = geo_level,
    is_cdp,
    cell_geoid,
    cell_level,
    cell_namelsad,
    rule
  ) |>
  slice_min(is_cdp, n = 1, by = c(key_city, key_state)) |>
  filter(n() == 1, .by = c(key_city, key_state))

# the "(balance)" places under their short names, where no place has that name
balance_lookup <-
  crosswalk_in |>
  filter(geo_level == "place", short_name != name) |>
  transmute(
    key_city = normalize_city(short_name),
    key_state,
    stusps,
    raw_geoid = geoid,
    raw_name = namelsad,
    raw_level = geo_level,
    is_cdp,
    cell_geoid,
    cell_level,
    cell_namelsad,
    rule
  ) |>
  anti_join(
    crosswalk_in |>
      filter(geo_level == "place") |>
      distinct(key_city = normalize_city(name), key_state),
    by = c("key_city", "key_state")
  ) |>
  filter(n() == 1, .by = c(key_city, key_state))

places_lookup <- bind_rows(places_lookup, balance_lookup)

county_subdivision_lookup <-
  crosswalk_in |>
  filter(geo_level == "county_subdivision", !str_ends(namelsad, " CCD")) |>
  transmute(
    key_city = normalize_city(name) |> str_remove(" town$"),
    key_state,
    stusps,
    raw_geoid = geoid,
    raw_name = namelsad,
    raw_level = geo_level,
    is_cdp,
    rank = case_when(
      str_ends(namelsad, " city") ~ 1,
      str_ends(namelsad, " borough") ~ 2,
      str_ends(namelsad, " village") ~ 3,
      str_ends(namelsad, " town") ~ 4,
      TRUE ~ 5
    ),
    cell_geoid,
    cell_level,
    cell_namelsad,
    rule,
    aland,
    key_alt = if_else(
      str_ends(namelsad, " township"),
      str_c(key_city, " township"),
      NA_character_
    )
  ) |>
  pivot_longer(
    c(key_city, key_alt),
    names_to = NULL,
    values_to = "key_city",
    values_drop_na = TRUE
  ) |>
  slice_min(rank, n = 1, by = c(key_city, key_state)) |>
  filter(n_distinct(cell_geoid) == 1, .by = c(key_city, key_state)) |>
  slice_max(aland, n = 1, with_ties = FALSE, by = c(key_city, key_state)) |>
  select(-rank, -aland)

reference_union <- bind_rows(places_lookup, county_subdivision_lookup)

counties_lookup <-
  arrow::read_parquet(
    str_c(shp_url, "/counties-sf.parquet"),
    col_select = c("geoid", "stusps", "name", "namelsad")
  ) |>
  left_join(state_names, by = "stusps") |>
  transmute(
    key_city = normalize_city(name),
    key_city_lsad = normalize_city(namelsad),
    key_state,
    stusps,
    raw_geoid = geoid,
    raw_name = name,
    raw_level = "county",
    is_cdp = NA,
    cell_geoid = geoid,
    cell_level = "county",
    cell_namelsad = name,
    rule = "county name"
  ) |>
  pivot_longer(
    c(key_city, key_city_lsad),
    names_to = NULL,
    values_to = "key_city"
  ) |>
  distinct() |>
  filter(n() == 1, .by = c(key_city, key_state))

manual_references <-
  readr::read_csv(
    "inputs/city-aliases-manual.csv",
    col_types = readr::cols(.default = "c")
  ) |>
  transmute(
    key_city = normalize_city(city),
    key_state = normalize_state(state),
    reference_name = normalize_city(reference_name),
    reference_state,
    reference_geoid,
    geo_level,
    display_name
  )

stopifnot(!anyDuplicated(manual_references[c("key_city", "key_state")]))

manual <-
  bind_rows(
    manual_references |>
      filter(!is.na(reference_geoid)) |>
      inner_join(
        crosswalk_in |>
          transmute(
            reference_geoid = geoid,
            stusps,
            raw_geoid = geoid,
            raw_name = namelsad,
            raw_level = geo_level,
            is_cdp,
            cell_geoid,
            cell_level,
            cell_namelsad,
            rule
          ),
        by = "reference_geoid"
      ),
    manual_references |>
      filter(is.na(reference_geoid), !geo_level %in% "county") |>
      inner_join(
        reference_union |>
          rename(reference_name = key_city, reference_state = key_state),
        by = c("reference_name", "reference_state"),
        relationship = "many-to-many"
      ) |>
      filter(is.na(geo_level) | geo_level == raw_level) |>
      filter(n_distinct(cell_geoid) == 1, .by = c(key_city, key_state)) |>
      slice_head(n = 1, by = c(key_city, key_state)),
    manual_references |>
      filter(is.na(reference_geoid), geo_level %in% "county") |>
      inner_join(
        counties_lookup |>
          rename(reference_name = key_city, reference_state = key_state),
        by = c("reference_name", "reference_state")
      )
  )

# Breaks if manual city aliases do not match exactly with a unique reference name and state
stopifnot(nrow(manual) == nrow(manual_references))

# Known non-place values excluded from matching; those that refer to geographic places are
# manually assigned to county per `inputs/city-county-manual.csv` in `city-county.R`
stoplist <-
  readr::read_csv("inputs/facility-stoplist.csv", show_col_types = FALSE) |>
  mutate(city = normalize_city(city))

cell_names <-
  crosswalk_in_all |>
  filter(!is.na(cell_geoid)) |>
  distinct(cell_geoid, cell_name = cell_short_name)

gnis <-
  arrow::read_parquet(
    str_c(shp_url, "/gnis-places-county-subdivisions-xwalk.parquet")
  ) |>
  left_join(state_names, by = "stusps") |>
  transmute(
    key_city = normalize_city(name),
    key_state,
    gnis_name = name,
    cell_geoid,
    in_state = cell_stusps == stusps
  )

# ==== Arrest keys =============================================================

arrest_cities <-
  arrow::read_parquet(
    "data/intermediate/arrests-latest-read.parquet",
    col_select = c("key_city", "key_state")
  ) |>
  count(key_city, key_state, name = "n_rows")

# ==== T0: triage ==============================================================

garbage_words <- c(
  "unknown",
  "unk",
  "unkown",
  "unkknown",
  "ukn",
  "uknown",
  "unkn",
  "unreported",
  "non provided",
  "not provided",
  "n/a",
  "na",
  "tba",
  "tbd",
  "other",
  "both",
  "at large",
  "us",
  "usa",
  "this"
)

t0 <-
  arrest_cities |>
  left_join(
    stoplist |>
      filter(!is.na(state)) |>
      distinct(key_city = city, key_state = state, stop_reason = reason),
    by = c("key_city", "key_state")
  ) |>
  left_join(
    stoplist |>
      filter(is.na(state)) |>
      distinct(key_city = city, stop_reason_any = reason),
    by = "key_city"
  ) |>
  mutate(
    stop_reason = coalesce(stop_reason, stop_reason_any),
    reason = case_when(
      is.na(key_city) ~ "missing city",
      !is.na(stop_reason) ~ str_c("facility/code: ", stop_reason),
      key_city %in% garbage_words ~ "garbage",
      str_detect(key_city, "^[a-z]$") ~ "garbage",
      str_detect(key_city, "^[0-9]{3,5}$") ~ "zip code",
      !str_detect(key_city, "[a-z]") ~ "garbage",
      str_detect(key_city, "^(suite|floor|basement|apt|unit|bldg)\\b") ~
        "address fragment"
    )
  ) |>
  filter(!is.na(reason)) |>
  transmute(
    key_city,
    key_state,
    n_rows,
    match_tier = "T0 triage",
    confidence = NA_character_,
    reason
  )

remainder <- arrest_cities |> anti_join(t0, by = c("key_city", "key_state"))

# ==== T1: manual ==============================================================

t1 <-
  remainder |>
  inner_join(
    manual |>
      select(
        key_city,
        key_state,
        stusps,
        raw_geoid,
        raw_name,
        raw_level,
        is_cdp,
        cell_geoid,
        cell_level,
        cell_namelsad,
        rule,
        display_name
      ),
    by = c("key_city", "key_state")
  ) |>
  mutate(match_tier = "T1 manual", confidence = "high")

remainder <- remainder |> anti_join(t1, by = c("key_city", "key_state"))

# ==== T2: exact place =========================================================

t2 <-
  remainder |>
  inner_join(places_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T2 place exact", confidence = "high")

remainder <- remainder |> anti_join(t2, by = c("key_city", "key_state"))

# ==== T3: exact county subdivision ===========================================

t3 <-
  remainder |>
  inner_join(county_subdivision_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T3 subdivision exact", confidence = "high")

remainder <- remainder |> anti_join(t3, by = c("key_city", "key_state"))

# ==== T4: exact county ========================================================

t4 <-
  remainder |>
  inner_join(counties_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T4 county exact", confidence = "high")

remainder <- remainder |> anti_join(t4, by = c("key_city", "key_state"))

# ==== T5: GNIS populated places, point within one cell =======================

gnis_candidates <-
  remainder |>
  filter(!is.na(key_city), !is.na(key_state)) |>
  inner_join(gnis, by = c("key_city", "key_state")) |>
  summarise(
    n_pts = n(),
    n_cells = n_distinct(cell_geoid[in_state %in% TRUE]),
    n_cells_other_state = n_distinct(cell_geoid[in_state %in% FALSE]),
    cell_geoid = first(cell_geoid[in_state %in% TRUE], default = NA_character_),
    .by = c(key_city, key_state, n_rows)
  ) |>
  left_join(
    crosswalk_in |> distinct(cell_geoid, cell_level, cell_namelsad),
    by = "cell_geoid",
    relationship = "many-to-one"
  ) |>
  left_join(declined_places, by = c("key_city", "key_state")) |>
  mutate(
    gnis_status = case_when(
      !is.na(place_note) ~ place_note,
      n_cells == 1 ~ "accepted",
      n_cells == 0 & n_cells_other_state > 0 ~ "cell in another state",
      n_cells == 0 ~ "not within a cell",
      TRUE ~ "ambiguous"
    )
  )

t5 <-
  gnis_candidates |>
  filter(gnis_status == "accepted") |>
  left_join(state_names, by = "key_state") |>
  transmute(
    key_city,
    key_state,
    n_rows,
    stusps,
    raw_geoid = NA_character_,
    raw_name = NA_character_,
    raw_level = NA_character_,
    is_cdp = NA,
    cell_geoid,
    cell_level,
    cell_namelsad,
    rule = "point in cell",
    match_tier = "T5 gnis",
    confidence = "medium"
  )

# Export subset of most common GNIS candidate values for manual review
gnis_candidates |>
  filter(n_rows >= gnis_review_min) |>
  arrange(desc(n_rows)) |>
  transmute(
    city = key_city,
    state = key_state,
    reference_name = if_else(gnis_status == "accepted", cell_namelsad, ""),
    reference_state = if_else(gnis_status == "accepted", key_state, ""),
    note = gnis_status,
    n_rows,
    n_pts
  ) |>
  readr::write_csv("inputs/gnis-review.csv")

remainder <- remainder |> anti_join(t5, by = c("key_city", "key_state"))

# ==== T6: residual ============================================================

t6 <-
  remainder |>
  transmute(
    key_city,
    key_state,
    n_rows,
    match_tier = "T6 unresolved",
    confidence = NA_character_,
    reason = "no match"
  )

stopifnot(nrow(t6) == nrow(remainder))

# print subset of most common remainder for manual review
unmatched <- bind_rows(t6, t0)

unmatched |>
  filter(!is.na(key_city), !is.na(key_state), n_rows >= manual_review_min) |>
  arrange(desc(n_rows)) |>
  transmute(
    city = key_city,
    state = key_state,
    n_rows
  ) |>
  print(n = Inf)

# ==== Assemble ================================================================

crosswalk_out <-
  bind_rows(t1, t2, t3, t4, t5, t0, t6) |>
  left_join(cell_names, by = "cell_geoid") |>
  transmute(
    key_city,
    key_state,
    n_rows,
    geoid = cell_geoid,
    geo_level = cell_level,
    matched_name = coalesce(display_name, cell_name, cell_namelsad),
    raw_geoid,
    raw_level,
    raw_name,
    is_cdp,
    rule,
    match_tier,
    confidence,
    reason
  ) |>
  arrange(match_tier, desc(n_rows))

stopifnot(
  !anyDuplicated(crosswalk_out[c("key_city", "key_state")]),
  nrow(crosswalk_out) == nrow(arrest_cities),
  all(
    is.na(crosswalk_out$geoid) |
      crosswalk_out$geoid %in%
        c(crosswalk_in$cell_geoid, counties_lookup$cell_geoid)
  )
)

arrow::write_parquet(crosswalk_out, "data/city-match-crosswalk.parquet")

# ==== Counts ==================================================================

crosswalk_out |>
  summarise(strings = n(), rows = sum(n_rows), .by = match_tier) |>
  mutate(pct_rows = round(100 * rows / sum(rows), 1)) |>
  print(n = Inf)

crosswalk_out |>
  filter(!is.na(geoid)) |>
  summarise(strings = n(), rows = sum(n_rows), .by = c(geo_level, rule)) |>
  arrange(desc(rows)) |>
  print(n = Inf)

crosswalk_out |>
  filter(rule %in% c("folded into subdivision", "lifted into place")) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, raw_name, matched_name, rule) |>
  print(n = 30)

crosswalk_out |>
  filter(match_tier == "T5 gnis") |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, matched_name, geo_level) |>
  print(n = 25)

crosswalk_out |>
  filter(match_tier == "T4 county exact") |>
  semi_join(gnis, by = c("key_city", "key_state")) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, matched_name) |>
  print(n = 30)

crosswalk_out |>
  filter(match_tier == "T6 unresolved", n_rows >= 20) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows) |>
  print(n = 30)

# END.
