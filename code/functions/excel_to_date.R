to_date <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, c("POSIXct","POSIXlt"))) return(as.Date(x))

  x_chr <- trimws(as.character(x))
  x_chr[x_chr %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  x_chr <- gsub(",", "", x_chr)

  x_num <- suppressWarnings(as.numeric(x_chr))
  is_excel <- !is.na(x_num) & x_num > 20000 & x_num < 60000

  out <- rep(as.Date(NA), length(x_chr))
  out[is_excel] <- as.Date(x_num[is_excel], origin = "1899-12-30")

  rem <- is.na(out) & !is.na(x_chr)
  if (any(rem)) {
    fmts <- c("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d")
    parsed <- rep(as.Date(NA), sum(rem))
    for (f in fmts) {
      tmp <- suppressWarnings(as.Date(x_chr[rem], format = f))
      parsed[is.na(parsed) & !is.na(tmp)] <- tmp[is.na(parsed) & !is.na(tmp)]
    }
    out[rem] <- parsed
  }

  out
}