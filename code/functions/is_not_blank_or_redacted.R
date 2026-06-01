library(dplyr)
library(stringr)

REDACT_PATTERN <- regex("\\(b\\)|\\(B\\)|b\\([0-9]\\)|B\\([0-9]\\)", ignore_case = TRUE)

is_redacted <- function(x) coalesce(str_detect(x, REDACT_PATTERN), FALSE)

is_not_blank_or_redacted <- function(x, min_nonredacted = 20) {
  if (is.character(x)) {
    vals <- str_squish(x)
    is_blank <- is.na(x) | vals == "" | vals == "NA"
    redacted <- str_detect(vals, REDACT_PATTERN)
    redacted[is.na(redacted)] <- FALSE

    is_meaningful <- !is_blank & !redacted
    n_meaningful <- sum(is_meaningful)
    n_redacted <- sum(redacted)

    if (n_meaningful == 0) return(FALSE)
    if (n_meaningful < min_nonredacted && n_redacted >= n_meaningful) return(FALSE)
    return(TRUE)
  } else {
    return(!all(is.na(x)))
  }
}

make_redactions_na <- function(df) {
  df |>
    mutate(across(
      where(is.character),
      ~ if_else(str_detect(str_squish(.x), REDACT_PATTERN), NA_character_, .x)
    ))
}
