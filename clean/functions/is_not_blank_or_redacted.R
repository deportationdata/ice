# Function to check if a column is not blank or redacted
is_not_blank_or_redacted <- function(x) {
  if (is.character(x)) {
    vals <- str_squish(x)
    # Check if all values are NA, all empty, or all redaction patterns
    redact_pattern <- regex("\\(b\\)|\\(B\\)|b\\([0-9]\\)|B\\([0-9]\\)", ignore_case = TRUE)
  
    # Handle NA values explicitly in str_detect
    redacted <- str_detect(vals, redact_pattern)
    redacted[is.na(redacted)] <- FALSE  # Treat NA as non-redacted for this check
  
    return(!all(is.na(x) | vals == "" | vals == "NA" | redacted))
  } else {
    # For non-character columns, keep them if not all NA
    return(!all(is.na(x)))
  }
}