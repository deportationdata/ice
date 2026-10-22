library(sf)
library(tidyverse)

source("code/functions/normalize_city.R")

# ==== References ==============================================================

shp_dir <- "~/github/us-shapefiles/data"
gnis_review_min <- 50

arrests_df <- arrow::read_parquet("data/arrests-latest.parquet")

state_names <-
  tibble(
    abb = c(state.abb, "DC", "PR", "GU", "VI", "MP", "AS"),
    full = str_to_lower(c(
      state.name,
      "District of Columbia",
      "Puerto Rico",
      "Guam",
      "United States Virgin Islands",
      "Commonwealth of the Northern Mariana Islands",
      "American Samoa"
    ))
  )

xwalk <-
  arrow::read_parquet(file.path(shp_dir, "places-cousubs-xwalk.parquet")) |>
  filter(!is.na(cell_geoid)) |>
  mutate(key_state = state_names$full[match(stusps, state_names$abb)])

entity_cols <- c(
  "stusps",
  "raw_geoid",
  "raw_name",
  "raw_level",
  "is_cdp",
  "cell_geoid",
  "cell_level",
  "cell_namelsad",
  "rule"
)

places_lookup <-
  xwalk |>
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
  group_by(key_city, key_state) |>
  filter(is_cdp == min(is_cdp)) |>
  filter(n_distinct(cell_geoid) == 1) |>
  ungroup() |>
  distinct(key_city, key_state, .keep_all = TRUE)

cousub_lookup <-
  xwalk |>
  filter(geo_level == "cousub", !str_ends(namelsad, " CCD")) |>
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
    rule
  ) |>
  (\(d) {
    bind_rows(
      d,
      d |>
        filter(str_ends(raw_name, " township")) |>
        mutate(key_city = str_c(key_city, " township"))
    )
  })() |>
  group_by(key_city, key_state) |>
  filter(rank == min(rank)) |>
  filter(n_distinct(cell_geoid) == 1) |>
  slice(1) |>
  ungroup() |>
  select(-rank)

ref_union <-
  bind_rows(places_lookup, cousub_lookup) |>
  distinct(key_city, key_state, .keep_all = TRUE)

manual_refs <-
  bind_rows(
    readr::read_csv(
      "inputs/city-place-manual.csv",
      col_types = readr::cols(.default = "c")
    ),
    readr::read_csv(
      "inputs/city-aliases-manual.csv",
      col_types = readr::cols(.default = "c")
    )
  ) |>
  transmute(
    key_city = normalize_city(city),
    key_state = normalize_state(state),
    ref_name = normalize_city(ref_name),
    ref_state,
    ref_geoid
  ) |>
  distinct(key_city, key_state, .keep_all = TRUE)

manual <-
  bind_rows(
    manual_refs |>
      filter(!is.na(ref_geoid)) |>
      inner_join(
        xwalk |>
          transmute(
            ref_geoid = geoid,
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
        by = "ref_geoid"
      ),
    manual_refs |>
      filter(is.na(ref_geoid)) |>
      inner_join(
        ref_union |> rename(ref_name = key_city, ref_state = key_state),
        by = c("ref_name", "ref_state")
      )
  )

stopifnot(nrow(manual) == nrow(manual_refs))

stoplist <-
  readr::read_csv("inputs/facility-stoplist.csv", show_col_types = FALSE) |>
  mutate(city = normalize_city(city))

processing_sites <-
  readr::read_csv("inputs/processing-sites.csv", show_col_types = FALSE) |>
  transmute(
    key_city = normalize_city(city),
    key_state = normalize_state(state),
    processing_site = TRUE
  )

counties <-
  arrow::read_parquet(
    file.path(shp_dir, "counties-sf.parquet"),
    col_select = c("stusps", "name")
  ) |>
  transmute(
    key_city = normalize_city(name),
    key_state = state_names$full[match(stusps, state_names$abb)]
  ) |>
  distinct()

gnis <-
  arrow::read_parquet(file.path(
    shp_dir,
    "gnis-places-cousubs-xwalk.parquet"
  )) |>
  transmute(
    key_city = normalize_city(name),
    key_state = state_names$full[match(stusps, state_names$abb)],
    gnis_name = name,
    cell_geoid,
    cell_level,
    cell_namelsad
  )

# ==== Arrest keys =============================================================

arrest_cities <-
  arrests_df |>
  transmute(
    key_city = na_if(normalize_city(apprehension_city), ""),
    key_state = normalize_state(apprehension_state_filled_in)
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

rem <- arrest_cities |> anti_join(t0, by = c("key_city", "key_state"))

stop()

# ==== T1: manual ==============================================================

t1 <-
  rem |>
  inner_join(
    manual |> select(key_city, key_state, all_of(entity_cols)),
    by = c("key_city", "key_state")
  ) |>
  mutate(match_tier = "T1 manual", confidence = "high")

rem <- rem |> anti_join(t1, by = c("key_city", "key_state"))

# ==== T2: exact place =========================================================

t2 <-
  rem |>
  inner_join(places_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T2 place exact", confidence = "high")

rem <- rem |> anti_join(t2, by = c("key_city", "key_state"))

# ==== T3: exact county subdivision ===========================================

t3 <-
  rem |>
  inner_join(cousub_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T3 cousub exact", confidence = "high")

rem <- rem |> anti_join(t3, by = c("key_city", "key_state"))

# ==== County names ============================================================

tc <-
  rem |>
  semi_join(counties, by = c("key_city", "key_state")) |>
  transmute(
    key_city,
    key_state,
    n_rows,
    match_tier = "T5 unresolved",
    confidence = NA_character_,
    reason = "county name"
  )

rem <- rem |> anti_join(tc, by = c("key_city", "key_state"))

# ==== T4: GNIS populated places, point within one cell =======================

gnis_status <-
  rem |>
  filter(!is.na(key_city), !is.na(key_state)) |>
  inner_join(gnis, by = c("key_city", "key_state")) |>
  group_by(key_city, key_state, n_rows) |>
  summarise(
    n_pts = n(),
    n_cells = n_distinct(cell_geoid, na.rm = TRUE),
    cell_geoid = first(cell_geoid, na_rm = TRUE),
    cell_level = first(cell_level, na_rm = TRUE),
    cell_namelsad = first(cell_namelsad, na_rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    gnis_status = case_when(
      n_cells == 1 ~ "accepted",
      n_cells == 0 ~ "not within a cell",
      TRUE ~ "ambiguous"
    )
  )

t4 <-
  gnis_status |>
  filter(gnis_status == "accepted") |>
  transmute(
    key_city,
    key_state,
    n_rows,
    stusps = state_names$abb[match(key_state, state_names$full)],
    raw_geoid = NA_character_,
    raw_name = NA_character_,
    raw_level = NA_character_,
    is_cdp = NA,
    cell_geoid,
    cell_level,
    cell_namelsad,
    rule = "point in cell",
    match_tier = "T4 gnis",
    confidence = "medium"
  )

gnis_status |>
  filter(n_rows >= gnis_review_min) |>
  arrange(desc(n_rows)) |>
  transmute(
    city = key_city,
    state = key_state,
    ref_name = if_else(gnis_status == "accepted", cell_namelsad, ""),
    ref_state = if_else(gnis_status == "accepted", key_state, ""),
    note = gnis_status,
    n_rows,
    n_pts
  ) |>
  readr::write_csv("inputs/gnis-review.csv")

rem <- rem |> anti_join(t4, by = c("key_city", "key_state"))

# ==== T5: residual ============================================================

t5 <-
  rem |>
  transmute(
    key_city,
    key_state,
    n_rows,
    match_tier = "T5 unresolved",
    confidence = NA_character_,
    reason = "no match"
  )

# ==== Assemble ================================================================

crosswalk <-
  bind_rows(t1, t2, t3, t4, t0, tc, t5) |>
  left_join(processing_sites, by = c("key_city", "key_state")) |>
  transmute(
    key_city,
    key_state,
    n_rows,
    geoid = cell_geoid,
    geo_level = cell_level,
    matched_name = cell_namelsad,
    raw_geoid,
    raw_level,
    raw_name,
    is_cdp,
    rule,
    match_tier,
    confidence,
    reason,
    processing_site = coalesce(processing_site, FALSE)
  ) |>
  arrange(match_tier, desc(n_rows))

stopifnot(
  !anyDuplicated(crosswalk[c("key_city", "key_state")]),
  nrow(crosswalk) == nrow(arrest_cities),
  all(is.na(crosswalk$geoid) | crosswalk$geoid %in% xwalk$cell_geoid)
)

readr::write_csv(crosswalk, "data/city-match-crosswalk.csv")

# ==== Counts ==================================================================

crosswalk |>
  group_by(match_tier) |>
  summarise(strings = n(), rows = sum(n_rows)) |>
  mutate(pct_rows = round(100 * rows / sum(rows), 1)) |>
  print(n = Inf)

crosswalk |>
  filter(!is.na(geoid)) |>
  group_by(geo_level, rule) |>
  summarise(strings = n(), rows = sum(n_rows), .groups = "drop") |>
  arrange(desc(rows)) |>
  print(n = Inf)

crosswalk |>
  filter(rule %in% c("folded into subdivision", "lifted into place")) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, raw_name, matched_name, rule) |>
  print(n = 30)

crosswalk |>
  filter(match_tier == "T4 gnis") |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows, matched_name, geo_level) |>
  print(n = 25)

crosswalk |>
  filter(match_tier == "T5 unresolved", n_rows >= 20) |>
  arrange(desc(n_rows)) |>
  select(key_city, key_state, n_rows) |>
  print(n = 30)
