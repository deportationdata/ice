library(arrow)
library(haven)
library(dplyr)

save_outputs <- function(df, basename) {
  df <- as_tibble(df)
  write_parquet(
    df,
    paste0("data/", basename, ".parquet"),
    compression = "zstd",
    compression_level = 19
  )
  dta_df <- if (any(nchar(names(df)) > 32)) {
    df |>
      rename_with(\(x) {
        x <- gsub("core_based_statistical_area", "cbsa", x)
        x <- gsub("federal_court_district_of_confinement", "fed_district", x)
        x <- gsub("federal_court_circuit_of_confinement", "fed_circuit", x)
        x <- gsub("detention_facility", "facility", x)
        make.unique(abbreviate(x, minlength = 32, strict = FALSE), sep = "_")
      })
  } else {
    df
  }
  write_dta(dta_df, paste0("data/", basename, ".dta"))
  write_sav(df, paste0("data/", basename, ".sav"))
  invisible(df)
}
