read_cell_names <- function(shp_dir) {
  new_england <- c("ME", "NH", "VT", "MA", "RI", "CT")
  arrow::read_parquet(
    file.path(shp_dir, "places-cousubs-sf.parquet"),
    col_select = c("geoid", "stusps", "name", "namelsad", "geo_level")
  ) |>
    dplyr::left_join(
      arrow::read_parquet(
        file.path(shp_dir, "places-cousubs-xwalk.parquet"),
        col_select = c("geoid", "geo_level", "common_name")
      ) |>
        dplyr::filter(geo_level == "place") |>
        dplyr::distinct(geoid, common_name),
      by = "geoid"
    ) |>
    dplyr::mutate(
      municipality = geo_level == "cousub" &
        (stringr::str_ends(namelsad, " city") |
          (stusps %in% new_england & stringr::str_ends(namelsad, " town"))),
      plain = stringr::str_remove(name, " Town$")
    ) |>
    dplyr::mutate(
      clash = municipality & sum(municipality) > 1,
      .by = c(stusps, plain)
    ) |>
    dplyr::transmute(
      cell_geoid = geoid,
      cell_level = geo_level,
      cell_name = dplyr::case_when(
        geo_level == "place" ~ common_name,
        municipality & !clash ~ plain,
        TRUE ~ namelsad
      )
    )
}
