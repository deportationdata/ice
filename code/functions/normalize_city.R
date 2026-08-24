# canonicalize raw city strings toward Census naming conventions, so exact
# joins hit ("Saint Paul" -> "st. paul", "Mt. Laurel" -> "mount laurel",
# "Laredo, Texas" -> "laredo"); applied identically when building the
# crosswalk and when joining it back

# trailing state suffixes people type into the city field
.state_suffix_re <- paste0(
  ",\\s*(",
  paste(
    c(
      tolower(state.name),
      tolower(state.abb),
      "dc",
      "d\\.c\\.",
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

normalize_city <- function(x) {
  x |>
    stringr::str_to_lower() |>
    stringr::str_squish() |>
    stringr::str_remove(.state_suffix_re) |>
    # "ft.pierce" -> "ft. pierce" so the prefix rules below apply
    stringr::str_replace("^(ft|mt|st)\\.(\\S)", "\\1. \\2") |>
    stringr::str_replace("^saint ", "st. ") |>
    stringr::str_replace("^st ", "st. ") |>
    stringr::str_replace("^ft\\.? ", "fort ") |>
    stringr::str_replace("^mt\\.? ", "mount ") |>
    stringr::str_replace("^n\\. ", "north ") |>
    stringr::str_replace("^s\\. ", "south ") |>
    stringr::str_replace("^e\\. ", "east ") |>
    stringr::str_replace("^w\\. ", "west ") |>
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
