# canonicalize raw city strings toward Census naming conventions, so exact
# joins hit ("Saint Paul" -> "st. paul", "Mt. Laurel" -> "mount laurel",
# "Laredo, Texas" -> "laredo"); applied identically when building the
# crosswalk and when joining it back

.state_abb_dotted <- paste0(
  substr(tolower(state.abb), 1, 1), "\\.?", substr(tolower(state.abb), 2, 2), "\\.?"
)

# trailing state suffixes people type into the city field
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
    # "ft.pierce" -> "ft. pierce" so the prefix rules below apply
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

# canonicalize state names to Census STATE_NAME conventions (lowercased)
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
