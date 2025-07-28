# examines a POSIXt object and checks if it represents a date with no time component and converts to just a Date object if so
check_dttm_and_convert_to_date <- function(x) {
  if(!inherits(x, "POSIXt")) {
   stop("Input must be a POSIXt object (datetime).")
  }
  if (all(lubridate::hour(x) == 0 & lubridate::minute(x) == 0 & lubridate::second(x) == 0, na.rm = TRUE)) {
    as.Date(x)
  } else {
    x
  }
}