# ---- Packages ----
library(tidyverse)
library(tidylog)
library(pointblank)

# ---- Functions ----
source("code/functions/check_dttm_and_convert_to_date.R")
source("code/functions/is_not_blank_or_redacted.R")

# ---- Read in files ----

col_types <- c(
  "text", # AOR
  "text", # County
  "text", # Apprehension Criminality
  "date", # Apprehension Date
  "text", # Event Landmark
  "text", # Apprehension Method
  "text", # State
  "text", # City
  "numeric", # Birth Year
  "text", # Case Category
  "text", # Case Status
  "text", # Case Threat Level
  "text", # Citizenship Country
  "date", # Departed Date
  "text", # Departure Country
  "date", # Final Order Date
  "text", # Final Order Yes No
  "text", # Final Program
  "text", # Final Program Group
  "text", # Gender
  "text", # Alien File Number
  "text" # Anonymized Unique Identifier
)

col_types_fy24 <- col_types[-21]

arrests_df <-
  list.files(
    here::here("inputs"),
    pattern = "^[^~].*ARS",
    full.names = TRUE
  ) |>
  set_names(basename) |>
  map_dfr(
    function(f) {
      # FY24 file lacks Alien File Number column
      n_cols <- ncol(readxl::read_excel(f, n_max = 0, skip = 6))
      ct <- if (n_cols == length(col_types)) {
        col_types
      } else {
        col_types_fy24
      }
      readxl::excel_sheets(f) |>
        set_names() |>
        map_dfr(
          function(s) {
            readxl::read_excel(
              path = f,
              sheet = s,
              col_types = ct,
              skip = 6
            )
          },
          .id = "sheet_original"
        )
    },
    .id = "file_original"
  )


# ---- Check: read ----
arrests_df |>
  col_exists(
    c(
      `Apprehension Date`,
      `Anonymized Unique Identifier`,
      `Gender`,
      `Case Status`
    )
  ) |>
  col_vals_not_null(
    `Apprehension Date`,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

us_state_names <- c(
  set_names(str_to_upper(state.name), state.abb),
  DC = "DISTRICT OF COLUMBIA",
  PR = "PUERTO RICO",
  GU = "GUAM",
  VI = "VIRGIN ISLANDS",
  MP = "NORTHERN MARIANA ISLANDS",
  AS = "AMERICAN SAMOA",
  FM = "FEDERATED STATES OF MICRONESIA"
)
us_abb_regex <- str_c(names(us_state_names), collapse = "|")

arrests_df <-
  arrests_df |>
  # clean names
  janitor::clean_names(allow_dupes = FALSE) |>
  # add row number from original file
  mutate(
    row_original = as.integer(row_number() + 6 + 1),
    .by = c("file_original", "sheet_original")
  ) |>
  # remove columns that are fully blank (all NA) or fully redacted
  select(where(is_not_blank_or_redacted)) |>
  # convert dttm to date if there is no time information in the column
  mutate(
    across(where(~ inherits(., "POSIXt")), check_dttm_and_convert_to_date)
  ) |>
  # replace redacted values with NA
  mutate(across(where(is.character), ~ na_if(.x, "b(6), b(7)c"))) |>
  mutate(across(where(is.character), ~ na_if(.x, "b(6), b(7)C"))) |>
  mutate(
    # create apprehension date without time
    apprehension_date_time = apprehension_date,
    apprehension_date = as.Date(apprehension_date_time),
    # convert birth year to integer
    birth_year = as.integer(birth_year),
    # standardized landmark for whole-landmark matching
    event_landmark_squished = str_squish(event_landmark |> str_to_upper())
  )

# per-landmark state distribution where state is present
landmark_state_counts <-
  arrests_df |>
  filter(!is.na(event_landmark_squished), !is.na(state)) |>
  count(event_landmark_squished, state) |>
  mutate(n_known = sum(n), share = n / sum(n), .by = event_landmark_squished)

# landmarks recorded in a single state at least 99% of the time -> state
landmark_states <-
  landmark_state_counts |>
  filter(n_known >= 20, share >= 0.99) |>
  select(event_landmark_squished, landmark_state = state)

# AORs recorded in a single state at least 99% of the time -> state
aor_states <-
  arrests_df |>
  filter(!is.na(aor), !is.na(state)) |>
  count(aor, state) |>
  filter(sum(n) >= 20, n / sum(n) >= 0.99, .by = aor) |>
  select(aor, aor_state = state)

arrests_df <-
  arrests_df |>
  left_join(
    landmark_states,
    by = "event_landmark_squished",
    relationship = "many-to-one"
  ) |>
  left_join(aor_states, by = "aor", relationship = "many-to-one") |>
  mutate(
    # pick last two-letter state abbreviation in the landmark, either:
    # 1. preceded by a comma ("GALVESTON CO JAIL, GALVESTON, TX") or
    # 2. a hyphen with a space ("RAPPAHANNOCK REGIONAL JAIL - VA"), or
    # 3. followed by "STATE" ("CAP - LEE COUNTY JAIL, AL STATE").
    # Do not merge just state because of ambiguities e.g. IN is preposition.
    landmark_state_abbr = str_extract(
      event_landmark_squished,
      str_c(
        ".*",
        "(?:,\\s*|-\\s+|\\b(?=(?:",
        us_abb_regex,
        ") STATE\\b))",
        "\\b(",
        us_abb_regex,
        ")\\b"
      ),
      group = 1
    ),
    abbr_state = unname(us_state_names[landmark_state_abbr])
  ) |>
  # how often this landmark's recorded state agrees with its typed
  # abbreviation; distrust the abbreviation when a well-observed landmark
  # disagrees too often (facility across a state line)
  left_join(
    landmark_state_counts |>
      select(event_landmark_squished, state, abbr_share = share),
    by = c("event_landmark_squished", "abbr_state" = "state"),
    relationship = "many-to-one"
  ) |>
  left_join(
    distinct(landmark_state_counts, event_landmark_squished, n_known),
    by = "event_landmark_squished",
    relationship = "many-to-one"
  ) |>
  mutate(
    abbr_state = if_else(
      coalesce(n_known, 0L) >= 20 & coalesce(abbr_share, 0) < 0.90,
      NA_character_,
      abbr_state
    ),
    apprehension_state_filled_in = coalesce(
      state,
      # first try the state abbreviation in the landmark field itself
      abbr_state,
      # if not that, use the state of landmarks that are >=99% same state - based on the state field
      landmark_state,
      # if not that, use the state of AORs that are >=99% in one state
      aor_state
    ),
    apprehension_state_fill_source = case_when(
      !is.na(state) ~ "original",
      !is.na(abbr_state) ~ "Abbreviation in landmark field",
      !is.na(landmark_state) ~ "Landmark appears in one state 99%",
      !is.na(aor_state) ~ "AOR appears in one state 99%",
    )
  ) |>
  select(
    -event_landmark_squished,
    -landmark_state_abbr,
    -abbr_state,
    -abbr_share,
    -n_known,
    -landmark_state,
    -aor_state
  )

# Simplified processed variables
arrests_df <-
  arrests_df |>
  mutate(
    apprehension_method_simple = case_when(
      apprehension_method %in%
        c(
          "Non-Custodial Arrest",
          "Located",
          "Probation and Parole",
          "Worksite Enforcement"
        ) ~ "At-Large Arrest",
      apprehension_method %in%
        c(
          "Custodial Arrest",
          "CAP Local Incarceration",
          "CAP Federal Incarceration",
          "CAP State Incarceration",
          "Criminal Alien Program"
        ) ~ "Custodial Arrest",
      apprehension_method == "287(g) Program" ~ "287(g) Program",
      TRUE ~ "Other"
    )
  )

# ---- Check: state imputation ----

arrests_df |>
  col_vals_expr(
    expr(is.na(state) | state == apprehension_state_filled_in),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  col_vals_expr(
    expr(
      is.na(apprehension_state_filled_in) ==
        is.na(apprehension_state_fill_source)
    ),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  specially(
    fn = ~ sum(!is.na(.$apprehension_state_filled_in)) > sum(!is.na(.$state)),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  specially(
    fn = ~ setequal(
      na.omit(unique(.$apprehension_state_filled_in)),
      na.omit(unique(.$state))
    ),
    actions = action_levels(warn_at = 1L, stop_at = 1L)
  ) |>
  invisible()

# ---- Check: clean ----
arrests_df |>
  col_exists(
    c(
      apprehension_date,
      anonymized_unique_identifier,
      gender,
      case_status,
      file_original,
      sheet_original,
      row_original,
      apprehension_date_time,
      birth_year
    )
  ) |>
  col_vals_not_null(
    row_original,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

# ---- Construct Duplicate Episode Indicators ----
library(data.table)
setDT(arrests_df)

arrests_df[,
  `:=`(
    anonymized_identifier_nona = fifelse(
      is.na(anonymized_unique_identifier),
      paste0("noid_", .I),
      anonymized_unique_identifier
    ),
    is_reprocessed = apprehension_method == "ERO Reprocessed Arrest"
  )
]

setorder(
  arrests_df,
  anonymized_identifier_nona,
  apprehension_date_time,
  is_reprocessed,
  file_original,
  sheet_original,
  row_original,
  na.last = TRUE
)

arrests_df[,
  duplicate_episode_identifier := {
    gap <- as.numeric(
      apprehension_date_time - shift(apprehension_date_time, type = "lag"),
      units = "hours"
    )
    cumsum(is.na(gap) | gap > 24)
  },
  by = anonymized_identifier_nona
]

arrests_df[,
  `:=`(
    duplicate_episode_first = seq_len(.N) == 1L,
    duplicate_likely = fifelse(
      is.na(anonymized_unique_identifier),
      as.logical(NA),
      .N > 1L
    )
  ),
  by = .(anonymized_identifier_nona, duplicate_episode_identifier)
]

arrests_df[, c("anonymized_identifier_nona", "is_reprocessed") := NULL]

arrests_df <-
  arrests_df |>
  as_tibble() |>
  mutate(duplicate_drop_row = duplicate_likely & !duplicate_episode_first)

# ---- Fill in state within duplicate arrest episodes ----
# rows of the same arrest episode (e.g. an ERO Reprocessed Arrest of the same
# event) sometimes have the state when another row does not; copy it over
# when all rows of the episode that have a state agree
episode_states <-
  arrests_df |>
  filter(
    !is.na(anonymized_unique_identifier),
    !is.na(apprehension_state_filled_in)
  ) |>
  distinct(
    anonymized_unique_identifier,
    duplicate_episode_identifier,
    episode_state = apprehension_state_filled_in
  ) |>
  filter(
    n() == 1,
    .by = c(anonymized_unique_identifier, duplicate_episode_identifier)
  )

arrests_df <-
  arrests_df |>
  left_join(
    episode_states,
    by = c("anonymized_unique_identifier", "duplicate_episode_identifier"),
    relationship = "many-to-one"
  ) |>
  mutate(
    apprehension_state_fill_source = if_else(
      is.na(apprehension_state_filled_in) & !is.na(episode_state),
      "episode",
      apprehension_state_fill_source
    ),
    apprehension_state_filled_in = coalesce(
      apprehension_state_filled_in,
      episode_state
    )
  ) |>
  select(-episode_state)

city_common_case_lookup <-
  arrests_df |>
  distinct(city) |>
  mutate(
    apprehension_city_common_case = city |>
      # insert a space when a period sits between two letters (e.g. "St.hospital"
      # -> "St. hospital", "FED.CORR." -> "FED. CORR.") so title-case and word
      # boundaries work properly downstream.
      str_replace_all("(?<=[A-Za-z])\\.(?=[A-Za-z])", ". ") |>
      # strip periods only from single-letter acronyms (U.S., C.F., L.E.C.) so
      # the single-letter-collapse step can re-join them. Preserve periods in
      # word abbreviations (St., Ste., Mt., Jr., Inc.).
      str_replace_all("(?<=\\b[A-Za-z])\\.", " ") |>
      str_to_lower() |>
      # drop stray typo characters ("C]HICAGO", "Berl;in", "Atlanta =")
      str_remove_all("[;=*?\\[\\]\\\\]") |>
      # slashes act as separators ("N/ Chesterfield", "Cape/Islands")
      str_replace_all("/", " ") |>
      # normalize all apostrophe-like characters to curly ’ — including the
      # straight ASCII ' and the 0x19 control byte (a mangled ’), otherwise
      # downstream problems with dealing with 's
      str_replace_all("[‘’`'\u0019]", "’") |>
      str_squish() |>
      # join single-letter apostrophe prefixes to the next word
      # (coeur d’ alene -> coeur d’alene)
      str_replace_all("\\b([a-z]’) (?=[a-z])", "\\1") |>
      # exception: Land O’ Lakes officially has a space after O’
      str_replace_all("\\bland o’lakes\\b", "land o’ lakes") |>
      # close up stray spaces around hyphens ("winston- salem" -> "winston-salem")
      str_replace_all("(?<=[a-z]) ?- ?(?=[a-z])", "-") |>
      # drop space before comma
      str_replace_all("\\s+,", ",") |>
      # fix , replacements re prefix words directly before a comma that should have had . ("St, X" -> "St X")
      str_replace_all(
        regex("\\b(st|ste|el|ft|mt|corpus)(\\.?),\\s*", ignore_case = TRUE),
        "\\1\\2 "
      ) |>
      # drop , at end of name
      str_replace_all(",\\s*$", "") |>
      # drop leading comma (", Birmingham" -> "Birmingham")
      str_remove("^,\\s*") |>
      # drop everything after a comma (", Birmingham, AL" -> "Birmingham")
      str_remove(",.*$") |>
      # ensure there is a space before an open parenthesis
      str_replace_all("([^ ])\\(", "\\1 (") |>
      # and after close parenthesis
      str_replace_all("\\)([^ ])", ") \\1") |>
      # drop trailing periods after full words ("beaufort." -> "beaufort") but
      # keep them on abbreviations of 3 letters or fewer ("cranberry twp.")
      str_replace("(?<=[a-z]{4})\\.+$", "") |>
      # collapse space-separated single letters into abbreviations (e.g. "u s" -> "US")
      str_replace_all("(?:^|(?<= ))([a-z]( [a-z])+)(?= |$)", function(m) {
        str_to_upper(str_replace_all(m, " ", ""))
      }) |>
      tools::toTitleCase() |>
      # capitalize letter after apostrophe (O'hare -> O'Hare). The apostrophe
      # normalization above makes everything curly, so this pattern must match
      # curly, not straight.
      str_replace_all("’([a-z])", function(m) {
        str_c("’", str_to_upper(str_sub(m, 2, 2)))
      }) |>
      # but keep possessive ’s lowercase (St. Luke’s)
      str_replace_all("’S\\b", "’s") |>
      # and Coeur d’Alene keeps a lowercase d’
      str_replace_all("\\b(Coeur|Couer) D’", "\\1 d’") |>
      # capitalize letter after "Mc" prefix (Mccreary -> McCreary)
      str_replace_all("\\bMc([a-z])", function(m) {
        str_c("Mc", str_to_upper(str_sub(m, 3, 3)))
      }) |>
      # close up spaced Mc names (Mc Lean -> McLean)
      str_replace_all("\\bMc ([A-Z])", "Mc\\1") |>
      # uppercase standalone single letters (initials like "g" in "Dale g Haile"),
      # leaving letters attached to an apostrophe alone
      str_replace_all("(?<!’)\\b([a-z])\\b(?!’)", str_to_upper) |>
      # add period to Saint abbreviations (St Luke's -> St. Luke's)
      str_replace_all("\\bSt\\b(?=\\s+[A-Z])", "St.") |>
      # change back to straight quotes throughout
      str_replace_all("[‘’]", "'") |>
      str_squish(),
    apprehension_city_common_case = if_else(
      !str_detect(apprehension_city_common_case, "[A-Za-z]") |
        str_to_lower(apprehension_city_common_case) %in%
          c("na", "n/a", "unk", "none"),
      NA_character_,
      apprehension_city_common_case
    )
  )

arrests_df <-
  arrests_df |>
  left_join(
    city_common_case_lookup,
    by = "city",
    relationship = "many-to-one"
  )

# ---- Check: duplicates ----
arrests_df |>
  col_exists(c(
    duplicate_likely,
    duplicate_episode_identifier,
    duplicate_episode_first,
    duplicate_drop_row
  )) |>
  col_vals_in_set(
    duplicate_likely,
    c(TRUE, FALSE, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

# ---- Pointblank Validation ----

arrests_df |>
  # -- Primary key / identifier checks --
  col_vals_not_null(
    anonymized_unique_identifier,
    actions = action_levels(warn_at = 0.02, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    apprehension_date,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Date range checks --
  col_vals_between(
    apprehension_date,
    as.Date("2022-09-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    departed_date,
    as.Date("2022-09-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    final_order_date,
    as.Date("1990-01-01"),
    Sys.Date(),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Birth year range --
  col_vals_between(
    birth_year,
    1900L,
    as.integer(format(Sys.Date(), "%Y")),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Categorical value checks --
  col_vals_in_set(
    gender,
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    apprehension_criminality,
    c(
      "1 Convicted Criminal",
      "2 Pending Criminal Charges",
      "3 Other Immigration Violator",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    final_order_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    case_status,
    c(
      "ACTIVE",
      "0-Withdrawal Permitted - I-275 Issued",
      "3-Voluntary Departure Confirmed",
      "5-Title 50 Expulsion",
      "6-Deported/Removed - Deportability",
      "7-Died",
      "8-Excluded/Deported/Removed",
      "8-Excluded/Removed - Inadmissibility",
      "9-VR Witnessed",
      "10-USC Prosecution Case Closed",
      "A-Proceedings Terminated",
      "B-Relief Granted",
      "E-Charging Document Canceled by ICE",
      "L-Legalization - Permanent Residence Granted",
      "Z-SAW - Permanent Residence Granted",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.05)
  ) |>
  col_vals_in_set(
    case_category,
    c(
      "[10] Visa Waiver Deportation / Removal",
      "[11] Administrative Deportation / Removal",
      "[12] Judicial Deportation / Removal",
      "[13] Section 250 Removal",
      "[14] Crewmen, Stowaways, S-Visa Holders, 235(c) Cases",
      "[15] Terrorist Court Case (Title 5)",
      "[16] Reinstated Final Order",
      "[17] USC Prosecution Case",
      "[1A] Voluntary Departure - Un-Expired and Un-Extended Departure Period",
      "[1B] Voluntary Departure - Extended Departure Period",
      "[1C] Expired Voluntary Departure Period - Referred to Investigation",
      "[2A] Deportable - Under Adjudication by IJ",
      "[2B] Deportable - Under Adjudication by BIA",
      "[2V] Voluntary Departure Granted by IJ",
      "[3] Deportable - Administratively Final Order",
      "[5A] Referred for Investigation - No Show for Hearing - No Final Order",
      "[5B] Removable - ICE Fugitive",
      "[5C] Relief Granted - Withholding of Deportation / Removal",
      "[5D] Final Order of Deportation / Removal - Deferred Action Granted",
      "[5E] Relief Granted - Extended Voluntary Departure",
      "[5F] Unable to Obtain Travel Document",
      "[8A] Excludable / Inadmissible - Hearing Not Commenced",
      "[8B] Excludable / Inadmissible - Under Adjudication by IJ",
      "[8C] Excludable / Inadmissible - Administrative Final Order Issued",
      "[8D] Excludable / Inadmissible - Under Adjudication by BIA",
      "[8E] Inadmissible - ICE Fugitive",
      "[8F] Expedited Removal",
      "[8G] Expedited Removal - Credible Fear Referral",
      "[8H] Expedited Removal - Status Claim Referral",
      "[8I] Inadmissible - ICE Fugitive - Expedited Removal",
      "[8K] Expedited Removal Terminated due to Credible Fear Finding / NTA Issued",
      "[8V] Voluntary Departure Granted by IJ",
      "[9] VR Under Safeguards",
      "[H] Historical Category For Migration Only",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Logical consistency: departed_date implies departure_country --
  col_vals_expr(
    expr(is.na(departed_date) | !is.na(departure_country)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Logical consistency: final_order_date only when final_order = YES --
  col_vals_expr(
    expr(is.na(final_order_date) | final_order_yes_no == "YES"),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- duplicate_likely should not be null when anonymized_unique_identifier is present --
  col_vals_expr(
    expr(is.na(anonymized_unique_identifier) | !is.na(duplicate_likely)),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# ---- Rename to match previous releases ----
arrests_df <-
  arrests_df |>
  rename(
    apprehension_aor = aor,
    apprehension_state_original = state,
    apprehension_city_original = city,
    unique_identifier = anonymized_unique_identifier
  ) |>
  relocate(
    apprehension_state_filled_in,
    apprehension_state_fill_source,
    .before = apprehension_state_original
  ) |>
  relocate(file_original, sheet_original, row_original, .after = last_col())

# ---- Save Outputs ----
arrow::write_parquet(
  arrests_df,
  "data/arrests-latest.parquet",
  compression = "zstd"
)
# writexl::write_xlsx(arrests_df, "data/arrests-latest.xlsx")
# haven::write_dta(arrests_df, "data/arrests-latest.dta")
# haven::write_sav(arrests_df, "data/arrests-latest.sav")

# END.
