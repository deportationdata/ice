# ---- Packages ----
library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

# ---- Input data ---
facilities_df <- arrow::read_parquet(
  "https://github.com/deportationdata/ice-detention-facilities/raw/refs/heads/main/data/facilities-latest.parquet"
) |>
  select(-field_office) |>
  distinct(detention_facility_code, .keep_all = TRUE)

# ---- Read in deduplicated stint-level data ----
detentions_df <-
  arrow::read_parquet("data/detention-stints-latest.parquet") |>
  filter(duplicate_drop_row == FALSE, !is.na(unique_identifier)) |>
  select(
    -starts_with("duplicate_"),
    -any_of(c(
      "city",
      "state",
      "county",
      "initial_bond_set_amount",
      "initial_bond_set_date",
      "bond_posted_amount",
      "bond_posted_date"
    ))
  ) |>
  as.data.table()

setorder(
  detentions_df,
  unique_identifier,
  book_in_date_time,
  book_out_date_time
)

# ---- Check: loaded stints ----
detentions_df |>
  col_exists(
    c(
      stay_ID,
      stint_ID,
      unique_identifier,
      detention_facility_code,
      book_in_date_time
    )
  ) |>
  rows_distinct(
    stint_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# ---- Create stay-level dataset ----
detentions_df[, first_stay := .I == .I[1], by = stay_ID]
detentions_df[, last_stay := .I == .I[.N], by = stay_ID]
detentions_df[,
  longest_stay := {
    book_out_date_time_imputed <- fifelse(
      is.na(book_out_date_time),
      as.POSIXct("2026-03-10 23:59:59", tz = "UTC"),
      book_out_date_time
    )
    diff <- book_out_date_time_imputed - book_in_date_time
    longest <- which(diff == max(diff, na.rm = TRUE))
    longest <- longest[length(longest)] # In case of ties, take the last one
    .I == .I[longest]
  },
  by = stay_ID
]

detention_facility_df_first <-
  detentions_df |>
  filter(first_stay == TRUE) |>
  select(
    stay_ID,
    detention_facility_first = detention_facility,
    detention_facility_code_first = detention_facility_code,
    book_in_date_time_first = book_in_date_time,
    book_out_date_time_first = book_out_date_time
  )

detention_facility_df_longest <-
  detentions_df |>
  filter(longest_stay == TRUE) |>
  select(
    stay_ID,
    detention_facility_longest = detention_facility,
    detention_facility_code_longest = detention_facility_code,
    book_in_date_time_longest = book_in_date_time,
    book_out_date_time_longest = book_out_date_time
  )

detention_facility_df_last <-
  detentions_df |>
  filter(last_stay == TRUE) |>
  select(
    stay_ID,
    detention_facility_last = detention_facility,
    detention_facility_code_last = detention_facility_code,
    book_in_date_time_last = book_in_date_time,
    book_out_date_time_last = book_out_date_time
  )

detention_stay_level_vars_df <-
  detentions_df[,
    c(
      list(
        n_stints = .N,
        detention_facility_codes_all = str_c(
          detention_facility_code,
          collapse = "; "
        ),
        initial_bond_set_amount_lowest_seen = if (
          all(is.na(initial_bond_set_amount_lowest_seen))
        ) {
          initial_bond_set_amount_lowest_seen[NA_integer_]
        } else {
          min(initial_bond_set_amount_lowest_seen, na.rm = TRUE)
        },
        initial_bond_set_date_earliest_seen = if (
          all(is.na(initial_bond_set_date_earliest_seen))
        ) {
          initial_bond_set_date_earliest_seen[NA_integer_]
        } else {
          min(initial_bond_set_date_earliest_seen, na.rm = TRUE)
        },
        bond_posted_amount_lowest_seen = if (
          all(is.na(bond_posted_amount_lowest_seen))
        ) {
          bond_posted_amount_lowest_seen[NA_integer_]
        } else {
          min(bond_posted_amount_lowest_seen, na.rm = TRUE)
        },
        bond_posted_date_earliest_seen = if (
          all(is.na(bond_posted_date_earliest_seen))
        ) {
          bond_posted_date_earliest_seen[NA_integer_]
        } else {
          min(bond_posted_date_earliest_seen, na.rm = TRUE)
        }
      ),
      lapply(
        .SD,
        function(x) tail(x, 1)
      )
    ),
    by = stay_ID,
    .SDcols = setdiff(
      names(detentions_df),
      c(
        "stay_ID",
        "detention_facility",
        "detention_facility_code",
        "book_in_date_time",
        "book_out_date_time",
        "detention_release_reason",
        "book_in_site",
        "book_in_aor",
        "initial_bond_set_amount_lowest_seen",
        "initial_bond_set_date_earliest_seen",
        "bond_posted_amount_lowest_seen",
        "bond_posted_date_earliest_seen"
      )
    )
  ]

detention_individual_level_vars_df <-
  detention_stay_level_vars_df[, .(n_stays = .N), by = unique_identifier]

detention_stays_df <-
  detentions_df |>
  distinct(stay_ID) |>
  left_join(detention_stay_level_vars_df, by = "stay_ID") |>
  left_join(detention_individual_level_vars_df, by = "unique_identifier") |>
  left_join(detention_facility_df_first, by = "stay_ID") |>
  left_join(detention_facility_df_longest, by = "stay_ID") |>
  left_join(detention_facility_df_last, by = "stay_ID") |>
  select(
    -first_stay,
    -last_stay,
    -longest_stay,
    -stint_ID,
    -file_original,
    -sheet_original,
    -row_original
  ) |>
  as_tibble()

# ---- Check: stay-level assembly ----
detention_stays_df |>
  col_exists(
    c(
      stay_ID,
      unique_identifier,
      n_stints,
      n_stays,
      detention_facility_code_first,
      detention_facility_code_longest,
      detention_facility_code_last,
      detention_facility_codes_all,
      book_in_date_time_first,
      book_in_date_time_longest,
      book_in_date_time_last
    )
  ) |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    n_stints,
    1L,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    n_stays,
    1L,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    detention_facility_code_first,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# ---- Pointblank Validation ----
detention_stays_df |>
  # -- Primary key: stay_ID should be unique and non-null --
  col_vals_not_null(
    stay_ID,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  rows_distinct(
    stay_ID,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    unique_identifier,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- n_stints and n_stays should be >= 1 --
  col_vals_gte(
    n_stints,
    1L,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    n_stays,
    1L,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Birth year range --
  col_vals_between(
    birth_year,
    1900L,
    as.integer(format(Sys.Date(), "%Y")),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  # -- Date range checks --
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
  # -- Categorical value checks --
  col_vals_in_set(
    gender,
    c("Male", "Female", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    marital_status,
    c("Divorced", "Married", "Separated", "Single", "Unknown", "Widowed", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    ethnicity,
    c("Hispanic Origin", "Not of Hispanic Origin", "Unknown", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    final_order_yes_no,
    c("YES", "NO", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    book_in_criminality,
    c(
      "1 Convicted Criminal",
      "2 Pending Criminal Charges",
      "3 Other Immigration Violator",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
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
      "[9] VR Under Safeguards",
      "[H] Historical Category For Migration Only",
      NA
    ),
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
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    felon,
    c(
      "Both (drug and other agg felon convictions)",
      "Drugs",
      "Not an Aggravated Felon",
      "Other",
      NA
    ),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    case_threat_level,
    c("1", "2", "3", "NA", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Bond amounts should be non-negative --
  col_vals_gte(
    initial_bond_set_amount_lowest_seen,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    bond_posted_amount_lowest_seen,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # -- Logical consistency: stay_book_in <= stay_book_out --
  col_vals_expr(
    expr(
      is.na(stay_book_in_date_time) |
        is.na(stay_book_out_date_time) |
        stay_book_in_date_time <= stay_book_out_date_time
    ),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- First stint book_in should match stay book_in --
  col_vals_expr(
    expr(
      is.na(book_in_date_time_first) |
        is.na(stay_book_in_date_time) |
        book_in_date_time_first == stay_book_in_date_time
    ),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- Last stint book_out should match stay book_out --
  col_vals_expr(
    expr(
      is.na(book_out_date_time_last) |
        is.na(stay_book_out_date_time) |
        book_out_date_time_last == stay_book_out_date_time
    ),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # -- First, longest, and last facility codes should not be null --
  col_vals_not_null(
    detention_facility_code_first,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    detention_facility_code_longest,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    detention_facility_code_last,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
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
  invisible()

# ---- Merge in facility info (city, state, county) for first/longest/last ----
n_before_facility_join <- nrow(detention_stays_df)
detention_stays_df <-
  detention_stays_df |>
  left_join(
    facilities_df |>
      select(detention_facility_code, city, state, county) |>
      rename_with(~ str_c(., "_first"), -detention_facility_code),
    by = c("detention_facility_code_first" = "detention_facility_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    facilities_df |>
      select(detention_facility_code, city, state, county) |>
      rename_with(~ str_c(., "_longest"), -detention_facility_code),
    by = c("detention_facility_code_longest" = "detention_facility_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    facilities_df |>
      select(detention_facility_code, city, state, county) |>
      rename_with(~ str_c(., "_last"), -detention_facility_code),
    by = c("detention_facility_code_last" = "detention_facility_code"),
    relationship = "many-to-one"
  )
stopifnot(nrow(detention_stays_df) == n_before_facility_join)

# ---- Sort by stay book-in date ----
detention_stays_df <- detention_stays_df |>
  arrange(stay_book_in_date_time)

# ---- Save Outputs ----
arrow::write_parquet(
  detention_stays_df,
  "data/detention-stays-latest.parquet",
  compression = "zstd"
)
detention_stays_df |>
  rename_with(
    ~ make.unique(
      abbreviate(.x, minlength = 32, strict = FALSE),
      sep = "_"
    )
  ) |>
  haven::write_dta("data/detention-stays-latest.dta")
haven::write_sav(detention_stays_df, "data/detention-stays-latest.sav")
detention_stays_df |>
  mutate(.chunk = ceiling(row_number() / 1e6)) |>
  group_split(.chunk, .keep = FALSE) |>
  set_names(~ str_c("Detention stays (Sheet ", seq_along(.x), ")")) |>
  writexl::write_xlsx("data/detention-stays-latest.xlsx")
