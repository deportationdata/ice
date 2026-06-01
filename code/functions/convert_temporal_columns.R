library(dplyr)
library(purrr)
library(stringr)
library(lubridate)
library(readr)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

standardize_missing <- function(x) {
  x %>%
    as.character() %>%
    str_trim() %>%
    na_if("") %>%
    na_if("NA") %>%
    na_if("N/A") %>%
    na_if("NULL") %>%
    na_if("UNK") %>%
    na_if("UNKNOWN")
}

is_excel_serial <- function(x_num) {
  !is.na(x_num) & x_num > 20000 & x_num < 60000
}

# ------------------------------------------------------------------------------
# Excel -> POSIXct (datetime)
# - Uses lubridate parsing orders
# - Safe elementwise fallback via purrr (no vapply)
# - Keeps POSIXct class
# ------------------------------------------------------------------------------

excel_to_datetime <- function(x, tz = "UTC") {
  x_chr <- standardize_missing(x)
  n <- length(x_chr)

  out_datetime <- rep(as.POSIXct(NA, tz = tz), n)

  x_num <- suppressWarnings(as.numeric(x_chr))
  excel_mask <- is_excel_serial(x_num)

  if (any(excel_mask)) {
    out_datetime[excel_mask] <- as.POSIXct(
      x_num[excel_mask] * 86400,
      origin = "1899-12-30",
      tz = tz
    )
  }

  needs_parse <- is.na(out_datetime) & !is.na(x_chr)
  if (any(needs_parse)) {
    parse_orders <- c(
      "Ymd HMS", "Ymd HM", "Ymd H", "Ymd",
      "mdY HMS", "mdY HM", "mdY H", "mdY",
      "dmy HMS", "dmy HM", "dmy H", "dmy",
      "Y-m-d H:M:S", "Y/m/d H:M:S", "m/d/Y H:M:S",
      "Y-m-dTHMSz", "ISO8601"
    )

    parsed <- suppressWarnings(parse_date_time(x_chr[needs_parse], orders = parse_orders, tz = tz))
    out_datetime[needs_parse] <- as.POSIXct(parsed, tz = tz)
  }

  still_needs_parse <- is.na(out_datetime) & !is.na(x_chr)
  if (any(still_needs_parse)) {
    fallback <- x_chr[still_needs_parse] %>%
      map(~ tryCatch(as.POSIXct(.x, tz = tz), error = function(e) as.POSIXct(NA, tz = tz))) %>%
      as.POSIXct(tz = tz)

    out_datetime[still_needs_parse] <- fallback
  }

  # Round up to nearest minute (preserves NA)
  non_missing <- !is.na(out_datetime)
  if (any(non_missing)) {
    out_datetime[non_missing] <- ceiling_date(out_datetime[non_missing], unit = "minute")
  }

  out_datetime
}

# ------------------------------------------------------------------------------
# Excel -> Date
# - Returns Date (NOT character) to preserve class (#10)
# ------------------------------------------------------------------------------

# Tidyverse-compliant version of your OLD behavior:
# - returns CHARACTER
# - converts parseable values to "YYYY-MM-DD"
# - leaves unparseable non-missing values unchanged (no data loss)
excel_to_date <- function(x, tz = "UTC") {
  x_chr <- standardize_missing(x)
  n <- length(x_chr)

  parsed_date <- rep(as.Date(NA), n)

  # Excel serials (possibly stored as text)
  x_num <- suppressWarnings(as.numeric(x_chr))
  is_excel <- !is.na(x_num) & x_num > 20000 & x_num < 60000
  if (any(is_excel)) {
    parsed_date[is_excel] <- as.Date(x_num[is_excel], origin = "1899-12-30")
  }

  # Parse remaining strings with lubridate
  needs_parse <- is.na(parsed_date) & !is.na(x_chr)
  if (any(needs_parse)) {
    orders <- c("Ymd", "mdY", "dmy", "Y-m-d", "Y/m/d", "m/d/Y", "d/m/Y", "b d Y")
    parsed <- suppressWarnings(parse_date_time(x_chr[needs_parse], orders = orders, tz = tz))
    parsed_date[needs_parse] <- as.Date(parsed)
  }

  # Elementwise safe fallback (purrr, no vapply)
  still_needs_parse <- is.na(parsed_date) & !is.na(x_chr)
  if (any(still_needs_parse)) {
    fallback_dates <- x_chr[still_needs_parse] %>%
      map(~ tryCatch(as.Date(.x), error = function(e) as.Date(NA))) %>%
      vctrs::vec_c(!!!.)

    parsed_date[still_needs_parse] <- fallback_dates
  }

  # Output stays character: replace only where a Date was parsed
  out <- x_chr
  parsed_mask <- !is.na(parsed_date)
  out[parsed_mask] <- format(parsed_date[parsed_mask], "%Y-%m-%d")

  out
}
# ------------------------------------------------------------------------------
# Convert temporal columns in a data frame (tidyverse patterns)
# - no grepl(): use str_detect()
# - keeps types (datetime -> POSIXct, date -> Date, year -> integer)
# ------------------------------------------------------------------------------

convert_df_temporal_columns <- function(data, tz = "UTC") {

  column_names <- names(data)

  datetime_cols <- column_names %>%
    keep(~ str_detect(.x, regex("_date", ignore_case = TRUE)) &
           str_detect(.x, regex("_time", ignore_case = TRUE)))

  date_cols <- column_names %>%
    keep(~ str_detect(.x, regex("_date", ignore_case = TRUE)) &
           !str_detect(.x, regex("_time", ignore_case = TRUE)))

  year_cols <- column_names %>%
    keep(~ str_detect(.x, regex("_year", ignore_case = TRUE)))

  data %>%
    mutate(
      across(all_of(datetime_cols), ~ excel_to_datetime(.x, tz = tz)),
      across(all_of(date_cols), ~ excel_to_date(.x, tz = tz)),
      across(all_of(year_cols), ~ suppressWarnings(as.integer(.x)))
    )
}
