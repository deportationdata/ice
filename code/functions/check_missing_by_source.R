check_missing_by_source <- function(
  df,
  by = c("file_original", "sheet_original"),
  action = c("stop", "warn")
) {
  action <- match.arg(action)
  na_share <-
    df |>
    summarise(
      across(-any_of(by), ~ mean(is.na(.x))),
      .by = all_of(by)
    ) |>
    pivot_longer(
      -all_of(by),
      names_to = "column",
      values_to = "share_na"
    )
  flagged <-
    na_share |>
    summarise(
      n_all_missing = sum(share_na == 1),
      n_sources = n(),
      .by = column
    ) |>
    filter(n_all_missing > 0, n_all_missing < n_sources)
  if (nrow(flagged) == 0) {
    return(invisible(df))
  }
  na_share |>
    filter(column %in% flagged$column) |>
    arrange(column, desc(share_na)) |>
    relocate(column) |>
    print(n = Inf, width = Inf)
  msg <- paste0(
    "Columns fully missing in some sources but not others: ",
    paste(flagged$column, collapse = ", ")
  )
  switch(action, stop = stop(msg, call. = FALSE), warn = warning(msg, call. = FALSE))
  invisible(df)
}
