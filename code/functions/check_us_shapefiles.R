check_us_shapefiles <- function(shp_dir) {
  expected <- readr::read_lines("inputs/us-shapefiles-commit.txt")
  git <- function(...) {
    system2("git", c("-C", shQuote(path.expand(shp_dir)), ...), stdout = TRUE)
  }
  actual <- git("rev-parse", "HEAD")
  dirty <- git("status", "--porcelain", "--", "data/")
  if (!identical(actual, expected)) {
    stop(
      "us-shapefiles is at ", substr(actual, 1, 7), " but the crosswalks were built with ",
      substr(expected, 1, 7), "; rebuild and update inputs/us-shapefiles-commit.txt"
    )
  }
  if (length(dirty) > 0) {
    stop(
      "us-shapefiles data/ has uncommitted changes:\n",
      paste(dirty, collapse = "\n")
    )
  }
  invisible(actual)
}
