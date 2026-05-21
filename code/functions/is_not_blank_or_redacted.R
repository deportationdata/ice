# Function to check if a column is not blank or redacted.
# Also drops columns where only a few non-redacted values "leaked" through
# heavy redaction (n_meaningful < min_nonredacted AND redaction dominates).
is_not_blank_or_redacted <- function(x, min_nonredacted = 20) {
  if (is.character(x)) {
    vals <- str_squish(x)
    redact_pattern <- regex("\\(b\\)|\\(B\\)|b\\([0-9]\\)|B\\([0-9]\\)", ignore_case = TRUE)

    is_blank <- is.na(x) | vals == "" | vals == "NA"
    redacted <- str_detect(vals, redact_pattern)
    redacted[is.na(redacted)] <- FALSE

    is_meaningful <- !is_blank & !redacted
    n_meaningful <- sum(is_meaningful)
    n_redacted <- sum(redacted)

    if (n_meaningful == 0) return(FALSE)
    if (n_meaningful < min_nonredacted && n_redacted >= n_meaningful) return(FALSE)
    return(TRUE)
  } else {
    # For non-character columns, keep them if not all NA
    return(!all(is.na(x)))
  }
}