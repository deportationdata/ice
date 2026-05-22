if_has <- function(df, cols, fn) {
  if (all(cols %in% names(df))) fn(df) else df
}
