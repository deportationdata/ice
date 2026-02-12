library(stringr)
library(lubridate)  

# Replace the excel_to_datetime implementation with this more robust parser
excel_to_datetime <- function(x, tz = "UTC") {
  x_chr <- trimws(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN")] <- NA_character_

  n <- length(x_chr)
  out <- rep(as.POSIXct(NA, tz = tz), n)

  # numeric (Excel serials possibly stored as text)
  x_num <- suppressWarnings(as.numeric(x_chr))
  is_excel <- !is.na(x_num) & x_num > 20000 & x_num < 60000
  if (any(is_excel)) {
    out[is_excel] <- as.POSIXct(x_num[is_excel] * 86400, origin = "1899-12-30", tz = tz)
  }

  # try lubridate parse_date_time for remaining non-NA strings
  to_parse <- is.na(out) & !is.na(x_chr)
  if (any(to_parse)) {
    orders <- c(
      "Ymd HMS", "Ymd HM", "Ymd H", "Ymd",
      "mdY HMS", "mdY HM", "mdY H", "mdY",
      "dmy HMS", "dmy HM", "dmy H", "dmy",
      "Y-m-d H:M:S", "Y/m/d H:M:S", "m/d/Y H:M:S",
      "Y-m-dTHMSz", "ISO8601"
    )
    parsed <- suppressWarnings(parse_date_time(x_chr[to_parse], orders = orders, tz = tz))
    out[to_parse] <- as.POSIXct(parsed, tz = tz)
  }

  # final elementwise safe fallback (won't stop on errors)
  to_parse2 <- is.na(out) & !is.na(x_chr)
  if (any(to_parse2)) {
    parsed2 <- vapply(
      x_chr[to_parse2],
      FUN = function(s) {
        result <- tryCatch(as.POSIXct(s, tz = tz), error = function(e) NA_real_)
        if (inherits(result, "POSIXt")) result else as.POSIXct(NA, tz = tz)
      },
      FUN.VALUE = as.POSIXct(NA, tz = tz)
    )
    out[to_parse2] <- parsed2
  }

  # Round up to the nearest minute (drops seconds). NAs are preserved.
  non_na <- !is.na(out)
  if (any(non_na)) {
    out[non_na] <- lubridate::ceiling_date(out[non_na], unit = "minute")
  }

  out
}

# ...existing code...
excel_to_date <- function(x, tz = "UTC") {
  x_chr <- trimws(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL", "UNK", "UNKNOWN")] <- NA_character_

  n <- length(x_chr)
  res <- as.Date(rep(NA, n))

  # Excel serials stored as numeric/text
  x_num <- suppressWarnings(as.numeric(x_chr))
  is_excel <- !is.na(x_num) & x_num > 20000 & x_num < 60000
  if (any(is_excel)) {
    res[is_excel] <- as.Date(x_num[is_excel], origin = "1899-12-30")
  }

  # Parse remaining strings with lubridate
  to_parse <- is.na(res) & !is.na(x_chr)
  if (any(to_parse)) {
    orders <- c("Ymd", "mdY", "dmy", "Y-m-d", "Y/m/d", "m/d/Y", "d/m/Y", "b d Y")
    parsed <- suppressWarnings(parse_date_time(x_chr[to_parse], orders = orders, tz = tz))
    parsed_date <- as.Date(parsed)
    res[to_parse] <- parsed_date
  }

  # Final elementwise safe fallback
  still <- is.na(res) & !is.na(x_chr)
  if (any(still)) {
    fallback <- vapply(
      x_chr[still],
      FUN = function(s) {
        tryCatch(as.Date(s), error = function(e) NA_Date_)
      },
      FUN.VALUE = as.Date(NA)
    )
    res[still] <- fallback
  }

  out <- x_chr
  is_date <- !is.na(res)
  out[is_date] <- as.character(res[is_date])

  out
}
# ...existing code...

convert_df_temporal_columns <- function(df0) {
  cols <- colnames(df0) # extract column names 

  datetime_cols <- cols[grepl("(?i)_date", cols) & grepl("(?i)_time", cols)]
  date_cols <- cols[grepl("(?i)_date", cols) & !grepl("(?i)_time", cols)] # outputs as str - some "_Date" cols have string objects
  year_cols <- cols[grepl("(?i)_year", cols)]

  # ...existing code...
  df <- dplyr::mutate(
    df0,
    dplyr::across(all_of(datetime_cols), ~ excel_to_datetime(.x, tz = "UTC")),
    dplyr::across(all_of(date_cols), ~ excel_to_date(.x)),
    dplyr::across(all_of(year_cols), ~ as.integer(.x))
  )


  return(df)
}

