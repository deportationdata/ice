library(dplyr)
library(purrr)

safe_bind_rows <- function(df1, df2) {
  shared <- intersect(names(df1), names(df2))
  mismatched <- shared[!map_lgl(
    shared, \(c) identical(class(df1[[c]]), class(df2[[c]]))
  )]
  df1 <- df1 |> mutate(across(all_of(mismatched), as.character))
  df2 <- df2 |> mutate(across(all_of(mismatched), as.character))
  bind_rows(df1, df2)
}
