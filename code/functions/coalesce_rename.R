library(dplyr)
library(rlang)

coalesce_rename <- function(df, new_name, old_name) {
  has_new <- new_name %in% names(df)
  has_old <- old_name %in% names(df)
  if (!has_old) return(df)
  if (!has_new) return(rename(df, !!new_name := !!old_name))
  df |>
    mutate(!!new_name := coalesce(.data[[new_name]], .data[[old_name]])) |>
    select(-all_of(old_name))
}
