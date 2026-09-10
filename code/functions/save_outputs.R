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
  write_sav(df, paste0("data/", basename, ".sav"))

  dta_df <-
    df |>
    mutate(across(
      where(is.character),
      \(x) {
        vals <- sort(unique(na.omit(x)))
        haven::labelled(
          match(x, vals),
          labels = setNames(seq_along(vals), vals)
        )
      }
    ))

  dta_df <- if (any(nchar(names(df)) > 32)) {
    dta_df |>
      # rename detention to dtn
      rename_with(
        ~ str_replace(.x, "detention", "dtn") |>
          str_replace("federal", "fed") |>
          str_replace("facility", "fcty") |>
          str_replace("_of_", "_") |>
          str_replace("district", "dst") |>
          str_replace("_confinement_", "_")
      ) |>
      rename_with(
        ~ make.unique(
          abbreviate(.x, minlength = 32, strict = FALSE),
          sep = "_"
        )
      )
  } else {
    dta_df
  }
  write_dta(dta_df, paste0("data/", basename, ".dta"))
  invisible(df)
}
