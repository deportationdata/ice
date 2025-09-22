library(tidyverse)
library(tidygeocoder)
library(sf)
library(sfarrow)

offices <- arrow::read_feather("data/ice-offices.feather")

if (file.exists("data/ice-offices-geocoded.feather")) {
  offices_geocoded <- sfarrow::st_read_feather(
    "data/ice-offices-geocoded.feather"
  )
} else {
  offices_geocoded <- tibble(
    office_name = character(),
    office_name_short = character(),
    agency = character(),
    field_office_name = character(),
    sub_office = logical(),
    address = character(),
    city = character(),
    state = character(),
    zip = character(),
    zip_4 = character(),
    address_full = character(),
    area = character(),
    office_latitude = numeric(),
    office_longitude = numeric()
  ) |>
    st_as_sf(
      coords = c("office_longitude", "office_latitude"),
      crs = 4326,
      remove = FALSE,
      agr = "constant",
      na.fail = FALSE,
      sf_column_name = "geometry_office"
    )
}

# are there new offices that need geocoding?
new_offices <- anti_join(
  offices,
  offices_geocoded |> distinct(office_name, address_full)
)

if (nrow(new_offices) == 0) {
  message("No new offices to geocode")
  quit(status = 0)
} else {
  message(paste("Geocoding", nrow(new_offices), "new offices"))
}

new_offices_geocoded <-
  new_offices |>
  geocode(
    address = address_full,
    method = "google",
    lat = office_latitude,
    long = office_longitude
  ) |>
  st_as_sf(
    coords = c("office_longitude", "office_latitude"),
    crs = 4326,
    remove = FALSE,
    agr = "constant",
    na.fail = FALSE,
    sf_column_name = "geometry_office"
  )

# combine with existing geocoded offices
all_offices_geocoded <-
  bind_rows(
    offices_geocoded,
    new_offices_geocoded
  )

sfarrow::st_write_feather(
  all_offices_geocoded,
  "data/ice-offices-geocoded.feather"
)
