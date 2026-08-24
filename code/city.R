library(sf)
library(tidyverse)
library(tidylog)
library(fuzzyjoin)

options(tigris_use_cache = TRUE)

source("code/functions/normalize_city.R")

# ==== References ==============================================================

arrests_df <- arrow::read_parquet("data/arrests-latest.parquet")

states_vec <- c(state.abb, "DC", "PR", "GU", "VI", "MP", "AS")

cb_places <-
  tigris::places(state = NULL, cb = TRUE, year = 2020)

cb_cousub <-
  purrr::map(
    states_vec,
    \(s) tigris::county_subdivisions(state = s, cb = TRUE, year = 2020)
  ) |>
  bind_rows()

cb_counties <-
  tigris::counties(state = NULL, cb = TRUE, year = 2020)

# township names repeat across counties within a state (dozens of "Washington
# township"s in PA), so for NAME-based matching keep only names unique in
# their state
cousub_sf_all <-
  cb_cousub |>
  filter(NAME != "County subdivisions not defined") |>
  transmute(
    key_state = str_to_lower(STATE_NAME),
    geoid = GEOID,
    matched_name = NAMELSAD,
    key_city = str_to_lower(NAME),
    geometry
  )

cousub_lookup <-
  cousub_sf_all |>
  as_tibble() |>
  add_count(key_city, key_state) |>
  filter(n == 1) |>
  select(-n) |>
  mutate(geo_level = "cousub")

ne_states <- str_to_lower(c(
  "Maine",
  "New Hampshire",
  "Vermont",
  "Massachusetts",
  "Rhode Island",
  "Connecticut"
))

# some place NAMEs repeat within a state (e.g. "Cottonwood city" and
# "Cottonwood CDP" in arizona); keep one row per (city, state), preferring
# incorporated places over CDPs, then larger land area, so joins never fan out
places_lookup <-
  cb_places |>
  transmute(
    key_city = str_to_lower(NAME),
    key_state = str_to_lower(STATE_NAME),
    geoid = GEOID,
    matched_name = NAMELSAD,
    is_cdp = str_ends(NAMELSAD, "CDP"),
    ALAND,
    geometry
  ) |>
  as_tibble() |>
  # in new england a town's village-center CDP shares the town's name; prefer
  # the town (the governing/ACS unit) by dropping the CDP so the cousub tier
  # claims the key. NY/midwest villages are real incorporated places and are
  # kept -- this applies to the six NE states only
  anti_join(
    cousub_lookup |>
      filter(key_state %in% ne_states) |>
      transmute(key_city, key_state, is_cdp = TRUE),
    by = c("key_city", "key_state", "is_cdp")
  ) |>
  arrange(key_city, key_state, is_cdp, desc(ALAND)) |>
  distinct(key_city, key_state, .keep_all = TRUE) |>
  transmute(
    key_city,
    key_state,
    geoid,
    matched_name,
    geo_level = "place",
    geometry
  )

# counties, matchable by bare name ("henrico") or full legal name
# ("brevard county", "calcasieu parish")
county_lookup <-
  cb_counties |>
  transmute(
    name = str_to_lower(NAME),
    namelsad = str_to_lower(NAMELSAD),
    key_state = str_to_lower(STATE_NAME),
    geoid = GEOID,
    matched_name = NAMELSAD,
    geometry
  ) |>
  as_tibble() |>
  pivot_longer(c(name, namelsad), values_to = "key_city") |>
  distinct(key_city, key_state, .keep_all = TRUE) |>
  transmute(
    key_city,
    key_state,
    geoid,
    matched_name,
    geo_level = "county",
    geometry
  )

# place + cousub union for the relaxed tiers (T6 aor repair, T7 fuzzy);
# where a name exists at both levels, the place wins
ref_union <-
  bind_rows(places_lookup, cousub_lookup) |>
  arrange(match(geo_level, c("place", "cousub"))) |>
  distinct(key_city, key_state, .keep_all = TRUE)

# which states each ICE area of responsibility covers, for checking and
# repairing the unreliable State field
aor_states <-
  arrow::read_parquet(
    "~/github/ice-offices/data/ice-aor-county-shp.parquet"
  ) |>
  transmute(
    aor = area_of_responsibility_name,
    aor_state = str_to_lower(STATE_NAME)
  ) |>
  distinct()

# ICE office locations: matched cities that are also office cities get
# flagged, since for custodial/office arrests City records the processing
# location rather than anything residence-like
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

ice_offices <-
  arrow::read_parquet("~/github/ice-offices/data/ice-offices.parquet") |>
  transmute(key_city = normalize_city(city), abb = state) |>
  left_join(state_names, by = "abb") |>
  transmute(key_city, key_state = full, is_ice_office_city = TRUE) |>
  distinct()

# hand-curated aliases: raw string -> census entity, resolved to geoid here
# so the csv stays plain and every row is validated on load
ref_by_name <-
  bind_rows(
    cb_places |>
      as_tibble() |>
      transmute(
        ref_name = str_to_lower(NAME),
        ref_state = str_to_lower(STATE_NAME),
        geoid = GEOID,
        matched_name = NAMELSAD,
        geo_level = "place",
        ALAND,
        geometry
      ),
    cb_cousub |>
      as_tibble() |>
      transmute(
        ref_name = str_to_lower(NAME),
        ref_state = str_to_lower(STATE_NAME),
        geoid = GEOID,
        matched_name = NAMELSAD,
        geo_level = "cousub",
        ALAND,
        geometry
      ),
    cb_counties |>
      as_tibble() |>
      transmute(
        ref_name = str_to_lower(NAME),
        ref_state = str_to_lower(STATE_NAME),
        geoid = GEOID,
        matched_name = NAMELSAD,
        geo_level = "county",
        ALAND,
        geometry
      )
  ) |>
  arrange(desc(ALAND)) |>
  distinct(ref_name, ref_state, geo_level, .keep_all = TRUE)

aliases <-
  readr::read_csv(
    "inputs/city-aliases-manual.csv",
    show_col_types = FALSE
  ) |>
  mutate(ref_name = str_to_lower(ref_name)) |>
  left_join(ref_by_name, by = c("ref_name", "ref_state", "geo_level"))

stopifnot("unresolved alias rows" = !anyNA(aliases$geoid))

# aliases that shadow a real same-named place: deliberate overrides
# (e.g. louisville -> metro balance); review this list when editing the csv
aliases |>
  semi_join(
    places_lookup,
    by = c("city" = "key_city", "state" = "key_state")
  ) |>
  select(city, state, matched_name, note) |>
  print()

stoplist <-
  readr::read_csv(
    "inputs/facility-stoplist.csv",
    show_col_types = FALSE
  )

# ==== Arrest keys =============================================================

arrest_cities <-
  arrests_df |>
  transmute(
    key_city = normalize_city(apprehension_city),
    key_state = normalize_state(apprehension_state_filled_in)
  ) |>
  count(key_city, key_state, name = "n_rows")

# ==== T0: triage -- non-locations out first ==================================

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
    bind_rows(
      stoplist |> filter(!is.na(state)),
      stoplist |>
        filter(is.na(state)) |>
        select(-state) |>
        cross_join(distinct(arrest_cities, state = key_state))
    ) |>
      distinct(key_city = city, key_state = state, stop_reason = reason),
    by = c("key_city", "key_state")
  ) |>
  mutate(
    reason = case_when(
      is.na(key_city) ~ "missing city",
      !is.na(stop_reason) ~ str_c("facility/code: ", stop_reason),
      key_city %in% garbage_words ~ "garbage",
      str_detect(key_city, "^[a-z]$") ~ "garbage",
      # no letters at all, except zip-shaped strings (those go to T5)
      !str_detect(key_city, "[a-z]") &
        !str_detect(key_city, "^[0-9]{3,5}$") ~ "garbage",
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

# ==== T1: hand-curated aliases ===============================================

t1 <-
  rem |>
  inner_join(
    aliases |>
      select(
        key_city = city,
        key_state = state,
        geoid,
        geo_level,
        matched_name,
        geometry
      ),
    by = c("key_city", "key_state")
  ) |>
  mutate(match_tier = "T1 alias", confidence = "high", city_dist = 0)

rem <- rem |> anti_join(t1, by = c("key_city", "key_state"))

# ==== T2: exact place ========================================================

t2 <-
  rem |>
  inner_join(places_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T2 place exact", confidence = "high", city_dist = 0)

rem <- rem |> anti_join(t2, by = c("key_city", "key_state"))

# ==== T3: exact county subdivision ===========================================

t3 <-
  rem |>
  inner_join(cousub_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T3 cousub exact", confidence = "high", city_dist = 0)

rem <- rem |> anti_join(t3, by = c("key_city", "key_state"))

# ==== T4: county name in the city field ======================================

t4 <-
  rem |>
  inner_join(county_lookup, by = c("key_city", "key_state")) |>
  mutate(match_tier = "T4 county", confidence = "medium", city_dist = 0)

rem <- rem |> anti_join(t4, by = c("key_city", "key_state"))

# ==== T5: zip codes typed into the city field ================================

# excel strips leading zeros, so pad back to 5 before lookup
t5 <-
  rem |>
  filter(str_detect(key_city, "^[0-9]{3,5}$")) |>
  mutate(zip = key_city) |>
  left_join(
    zipcodeR::zip_code_db |>
      transmute(
        zip = zipcode,
        z_city = normalize_city(major_city),
        abb = state
      ) |>
      left_join(state_names, by = "abb") |>
      transmute(zip, z_city, z_state = full),
    by = "zip"
  ) |>
  inner_join(
    ref_union,
    by = c("z_city" = "key_city", "z_state" = "key_state")
  ) |>
  mutate(match_tier = "T5 zip", confidence = "medium", city_dist = 0) |>
  select(-zip, -z_city, -z_state)

rem <- rem |> anti_join(t5, by = c("key_city", "key_state"))

# ==== T6: repair implausible states using the arrest AOR =====================

# the State field is unreliable (Detroit / MASSACHUSETTS, CHICAGO / IOWA):
# where the recorded State is outside the AOR's states, retry the city in the
# AOR's own states, accepting only an unambiguous single candidate
t6 <-
  arrests_df |>
  transmute(
    key_city = normalize_city(apprehension_city),
    key_state = normalize_state(apprehension_state_filled_in),
    aor = apprehension_aor |>
      str_remove(" Area of Responsibility$")
  ) |>
  distinct() |>
  semi_join(rem, by = c("key_city", "key_state")) |>
  anti_join(aor_states, by = c("aor" = "aor", "key_state" = "aor_state")) |>
  inner_join(aor_states, by = "aor", relationship = "many-to-many") |>
  inner_join(
    ref_union |> rename(matched_state = key_state),
    by = c("key_city" = "key_city", "aor_state" = "matched_state")
  ) |>
  group_by(key_city, key_state) |>
  filter(n() == 1) |>
  ungroup() |>
  left_join(
    rem |> select(key_city, key_state, n_rows),
    by = c("key_city", "key_state")
  ) |>
  transmute(
    key_city,
    key_state,
    n_rows,
    geoid,
    geo_level,
    matched_name,
    matched_state = aor_state,
    geometry,
    match_tier = "T6 aor state repair",
    confidence = "medium",
    city_dist = 0
  )

rem <- rem |> anti_join(t6, by = c("key_city", "key_state"))

# ==== T7: fuzzy, blocked by state, two bands =================================

# jaro-winkler within the recorded state against place+cousub; <= 0.08 is
# auto-accepted, 0.08-0.15 goes to the review file only -- approved rows are
# then added to city-aliases.csv so no string is reviewed twice
fuzzy_all <-
  rem |>
  filter(!is.na(key_city), !is.na(key_state), n_rows >= 2) |>
  group_by(key_state) |>
  group_modify(\(d, g) {
    ref_s <-
      ref_union |>
      filter(key_state == g$key_state) |>
      select(ref_city = key_city, geoid, geo_level, matched_name, geometry)
    if (nrow(ref_s) == 0) {
      return(tibble())
    }
    stringdist_inner_join(
      d,
      ref_s,
      by = c("key_city" = "ref_city"),
      method = "jw",
      p = 0.1,
      max_dist = 0.15,
      distance_col = "city_dist"
    )
  }) |>
  ungroup() |>
  arrange(match(geo_level, c("place", "cousub"))) |>
  group_by(key_city, key_state) |>
  slice_min(city_dist, n = 1, with_ties = FALSE) |>
  ungroup()

t7 <-
  fuzzy_all |>
  filter(city_dist <= 0.08) |>
  mutate(match_tier = "T7 fuzzy", confidence = "medium") |>
  select(-ref_city)

fuzzy_all |>
  filter(city_dist > 0.08) |>
  arrange(desc(n_rows)) |>
  select(
    key_city,
    key_state,
    n_rows,
    suggested = matched_name,
    geoid,
    geo_level,
    city_dist
  ) |>
  readr::write_csv("inputs/fuzzy-review.csv")

rem <- rem |> anti_join(t7, by = c("key_city", "key_state"))

# ==== T8: geocode the tail, assign by point-in-polygon =======================

# the polygon, not the name, assigns the geoid, so vernacular names resolve
# to their true place/cousub/county. results are cached so reruns are free;
# raise geocode_max_new to work further down the tail
geocode_max_new <- 250
geocode_cache_path <- "data/geocoded-cities.csv"

geocode_cache <-
  if (file.exists(geocode_cache_path)) {
    readr::read_csv(geocode_cache_path, show_col_types = FALSE)
  } else {
    tibble(query = character(), lat = numeric(), long = numeric())
  }

geocode_new <-
  rem |>
  filter(!is.na(key_city), !is.na(key_state), n_rows >= 2) |>
  arrange(desc(n_rows)) |>
  mutate(query = str_c(key_city, ", ", key_state)) |>
  anti_join(geocode_cache, by = "query") |>
  slice_head(n = geocode_max_new)

if (nrow(geocode_new) > 0) {
  geocode_cache <-
    geocode_new |>
    select(query) |>
    tidygeocoder::geocode(address = query, method = "arcgis") |>
    bind_rows(geocode_cache) |>
    distinct(query, .keep_all = TRUE)
  readr::write_csv(geocode_cache, geocode_cache_path)
}

geocoded_pts <-
  rem |>
  filter(!is.na(key_city), !is.na(key_state)) |>
  mutate(query = str_c(key_city, ", ", key_state)) |>
  inner_join(geocode_cache, by = "query") |>
  filter(!is.na(lat)) |>
  st_as_sf(coords = c("long", "lat"), crs = 4326) |>
  st_transform(st_crs(cb_places))

pip_levels <- list(
  place = cb_places |>
    transmute(geoid = GEOID, matched_name = NAMELSAD, geo_level = "place"),
  cousub = cousub_sf_all |>
    transmute(geoid, matched_name, geo_level = "cousub"),
  county = cb_counties |>
    transmute(geoid = GEOID, matched_name = NAMELSAD, geo_level = "county")
)

t8 <- tibble()
pip_rem <- geocoded_pts
for (lev in pip_levels) {
  hit <- st_join(pip_rem, lev, left = FALSE)
  t8 <- bind_rows(t8, as_tibble(hit) |> select(-geometry))
  pip_rem <- pip_rem |>
    anti_join(as_tibble(hit), by = c("key_city", "key_state"))
}
t8 <-
  t8 |>
  # a point on a boundary can fall in two polygons; keep one
  distinct(key_city, key_state, .keep_all = TRUE) |>
  transmute(
    key_city,
    key_state,
    n_rows,
    geoid,
    geo_level,
    matched_name,
    match_tier = "T8 geocode",
    confidence = "medium",
    city_dist = NA_real_
  )

rem <- rem |> anti_join(t8, by = c("key_city", "key_state"))

# ==== T9: honest residual ====================================================

t9 <-
  rem |>
  transmute(
    key_city,
    key_state,
    n_rows,
    match_tier = "T9 unresolved",
    confidence = NA_character_,
    reason = "no match"
  )

# ==== Assemble the crosswalk =================================================

tiers <- list(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9)

city_crosswalk <-
  tiers |>
  purrr::map(\(d) d |> as_tibble() |> select(-any_of("geometry"))) |>
  bind_rows() |>
  mutate(
    matched_state = coalesce(
      matched_state,
      if_else(!is.na(geoid), key_state, NA_character_)
    )
  ) |>
  left_join(ice_offices, by = c("key_city", "key_state")) |>
  mutate(is_ice_office_city = coalesce(is_ice_office_city, FALSE)) |>
  select(
    key_city,
    key_state,
    n_rows,
    geoid,
    geo_level,
    matched_name,
    matched_state,
    match_tier,
    confidence,
    city_dist,
    reason,
    is_ice_office_city
  ) |>
  arrange(match_tier, desc(n_rows))

# invariants: one row per key, every arrest key present exactly once
stopifnot(
  !anyDuplicated(city_crosswalk[c("key_city", "key_state")]),
  nrow(city_crosswalk) == nrow(arrest_cities)
)

city_crosswalk |>
  pointblank::create_agent() |>
  pointblank::rows_distinct(pointblank::vars(key_city, key_state)) |>
  pointblank::col_vals_in_set(
    geo_level,
    set = c("place", "cousub", "county", NA)
  ) |>
  pointblank::col_vals_not_null(match_tier) |>
  pointblank::interrogate()

readr::write_csv(city_crosswalk, "data/city-place-crosswalk.csv")

# matched entities with geometry, for mapping
city_matches_sf <-
  tiers |>
  purrr::keep(\(d) "geometry" %in% names(d)) |>
  purrr::map(\(d) {
    select(
      d,
      key_city,
      key_state,
      n_rows,
      geoid,
      geo_level,
      matched_name,
      match_tier,
      geometry
    )
  }) |>
  bind_rows() |>
  st_as_sf()

# ==== Coverage report ========================================================

city_crosswalk |>
  group_by(match_tier) |>
  summarise(strings = n(), rows = sum(n_rows)) |>
  mutate(pct_rows = round(100 * rows / sum(rows), 1)) |>
  print(n = Inf)

# review queues, most impactful first
city_crosswalk |>
  filter(match_tier == "T9 unresolved", n_rows >= 5) |>
  arrange(desc(n_rows)) |>
  print(n = 30)

# ==== Join back onto the arrests data ========================================

arrests_df_match <-
  arrests_df |>
  # drop leftovers from earlier runs so rerunning never creates .x/.y columns
  select(
    -any_of(c(
      "city_geoid",
      "city_geo_level",
      "city_matched_name",
      "city_match_tier",
      "city_match_confidence",
      "is_ice_office_city",
      "match_status",
      "state_in_aor",
      "city_match_type"
    ))
  ) |>
  mutate(
    key_city = normalize_city(apprehension_city),
    key_state = normalize_state(apprehension_state_filled_in),
    aor = apprehension_aor |>
      str_remove(" Area of Responsibility$") |>
      str_remove_all(fixed("."))
  ) |>
  left_join(
    city_crosswalk |>
      select(
        key_city,
        key_state,
        city_geoid = geoid,
        city_geo_level = geo_level,
        city_matched_name = matched_name,
        city_match_tier = match_tier,
        city_match_confidence = confidence,
        is_ice_office_city
      ),
    by = c("key_city", "key_state")
  ) |>
  # flag rows whose recorded State is outside the AOR's states (FALSE also
  # when AOR or State is missing, i.e. not verifiable)
  left_join(
    aor_states |> mutate(state_in_aor = TRUE),
    by = c("aor" = "aor", "key_state" = "aor_state")
  ) |>
  mutate(
    state_in_aor = coalesce(state_in_aor, FALSE),
    match_status = case_when(
      is.na(apprehension_city) &
        is.na(apprehension_state_filled_in) ~ "missing info",
      !is.na(city_geoid) ~ "matched",
      TRUE ~ "unmatched"
    )
  ) |>
  select(-key_city, -key_state, -aor)

stopifnot(nrow(arrests_df_match) == nrow(arrests_df))

# ==== Diagnostics ============================================================

arrests_df_match |>
  count(match_status, city_match_tier) |>
  print(n = Inf)

# step 0 semantics: are office-city rows a different kind of record?
arrests_df_match |>
  count(is_ice_office_city, apprehension_method, sort = TRUE) |>
  print(n = 30)

# suspect matches: matched in the recorded state, but that state disagrees
# with the AOR -- may be the right name in the wrong state
arrests_df_match |>
  filter(!state_in_aor, match_status == "matched") |>
  count(
    apprehension_city,
    apprehension_state_filled_in,
    city_match_tier,
    sort = TRUE
  ) |>
  print(n = 20)
