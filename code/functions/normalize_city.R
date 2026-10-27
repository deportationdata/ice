.state_abb_dotted <- paste0(
  substr(tolower(state.abb), 1, 1), "\\.?", substr(tolower(state.abb), 2, 2), "\\.?"
)

.state_suffix_re <- paste0(
  ",\\s*(",
  paste(
    c(
      tolower(state.name),
      .state_abb_dotted,
      "district of columbia",
      "pr",
      "puerto rico",
      "usvi",
      "usa",
      "us"
    ),
    collapse = "|"
  ),
  ")\\.?$"
)

.state_abb_wordlike <- c("in", "or", "me", "hi", "oh", "ok", "de", "la", "co")

.state_abb_suffix_re <- paste0(
  "\\s+(",
  paste(
    .state_abb_dotted[!tolower(state.abb) %in% .state_abb_wordlike],
    collapse = "|"
  ),
  ")\\.?$"
)

normalize_city <- function(x) {
  x |>
    stringi::stri_trans_general("Latin-ASCII") |>
    stringr::str_to_lower() |>
    stringr::str_replace_all("[\u2019\u2018'`]", " ") |>
    stringr::str_replace_all("-", " ") |>
    stringr::str_squish() |>
    stringr::str_remove("[,.;:]+$") |>
    stringr::str_squish() |>
    stringr::str_replace("(^|,\\s*|\\s+)d\\.?c$", "\\1dc") |>
    stringr::str_replace(",\\s*dc$", " dc") |>
    stringr::str_remove(.state_suffix_re) |>
    stringr::str_remove(.state_abb_suffix_re) |>
    stringr::str_replace("^(ft|mt|st)\\.(\\S)", "\\1. \\2") |>
    stringr::str_replace_all("\\bsainte ", "ste. ") |>
    stringr::str_replace_all("\\bsaint ", "st. ") |>
    stringr::str_replace_all("\\bst\\.? ", "st. ") |>
    stringr::str_replace_all("\\bste\\.? ", "ste. ") |>
    stringr::str_replace("^ft\\.? ", "fort ") |>
    stringr::str_replace("^mt\\.? ", "mount ") |>
    stringr::str_replace("^n\\.? ", "north ") |>
    stringr::str_replace("^s\\.? ", "south ") |>
    stringr::str_replace("^e\\.? ", "east ") |>
    stringr::str_replace("^w\\.? ", "west ") |>
    stringr::str_replace(" twp\\.?$", " township") |>
    stringr::str_squish()
}

.state_fixes <- c(
  "virgin islands" = "united states virgin islands",
  "u.s. virgin islands" = "united states virgin islands",
  "us virgin islands" = "united states virgin islands",
  "northern mariana islands" = "commonwealth of the northern mariana islands"
)

normalize_state <- function(x) {
  x <- stringr::str_to_lower(stringr::str_squish(x))
  dplyr::coalesce(unname(.state_fixes[x]), x)
}

state_names <-
  dplyr::tibble(
    stusps = c(state.abb, "DC", "PR", "GU", "VI", "MP", "AS"),
    key_state = stringr::str_to_lower(c(
      state.name,
      "District of Columbia",
      "Puerto Rico",
      "Guam",
      "United States Virgin Islands",
      "Commonwealth of the Northern Mariana Islands",
      "American Samoa"
    ))
  )
