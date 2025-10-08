library(tidyverse)
library(sf)
library(tigris)
library(sfarrow)

counties_sf <- counties(
  cb = TRUE,
  year = 2024,
  class = "sf",
  progress = FALSE
)

aor_df <- read_csv("inputs/ice-areas-of-responsibility.csv")

aor_entire_states <-
  aor_df |>
  filter(is.na(county_name)) |>
  select(-county_name)

aor_partial_states <- aor_df |> filter(!is.na(county_name))

aor_county_sf_partial_states <-
  counties_sf |>
  inner_join(
    aor_partial_states,
    by = c("NAME" = "county_name", "STATE_NAME" = "state_name")
  )

aor_county_sf_entire_states <-
  counties_sf |>
  inner_join(
    aor_entire_states,
    by = c("STATE_NAME" = "state_name")
  )

aor_county_sf <-
  bind_rows(
    aor_county_sf_partial_states,
    aor_county_sf_entire_states
  ) |>
  mutate(
    area_of_responsibility_name = office_name
  )

# validate that all counties are assigned to an AOR except in American Samoa
# counties_sf |>
#   as_tibble() |>
#   left_join(
#     aor_sf |> mutate(in_aor_df = 1) |> as_tibble(),
#     by = c("NAME", "STATE_NAME")
#   ) |>
#   filter(is.na(office_name)) |>
#   select(1:10)

hq_aor_df <- tibble(area_of_responsibility_name = "HQ")

aor_county_sf <-
  bind_rows(aor_county_sf, hq_aor_df)

sfarrow::st_write_feather(aor_county_sf, "data/ice-aor-county-shp.feather")

temp_dir <- tempdir()
temp_shp_path <- file.path(temp_dir, "ice-aor-county-shp.shp")
st_write(
  aor_sf |> rename(aor_name = area_of_responsibility_name),
  temp_shp_path,
  append = FALSE
)

# create a zip file with all necessary shapefile components
zip(
  zipfile = "data/ice-aor-county-shp.zip",
  files = c(
    file.path(temp_dir, "ice-aor-county-shp.shp"),
    file.path(temp_dir, "ice-aor-county-shp.dbf"),
    file.path(temp_dir, "ice-aor-county-shp.prj"),
    file.path(temp_dir, "ice-aor-county-shp.shx")
  )
)

stop()

aor_sf <-
  aor_county_sf |>
  filter(!is.na(area_of_responsibility_name)) |>
  group_by(office_name, area_of_responsibility_name) |>
  summarize(geometry = st_union(geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON")

sfarrow::st_write_feather(aor_sf, "data/ice-aor-shp.feather")

temp_dir <- tempdir()
temp_shp_path <- file.path(temp_dir, "ice-aor-shp.shp")
st_write(
  aor_sf |> rename(aor_name = area_of_responsibility_name),
  temp_shp_path,
  append = FALSE
)

# create a zip file with all necessary shapefile components
zip(
  zipfile = "data/ice-aor-shp.zip",
  files = c(
    file.path(temp_dir, "ice-aor-shp.shp"),
    file.path(temp_dir, "ice-aor-shp.dbf"),
    file.path(temp_dir, "ice-aor-shp.prj"),
    file.path(temp_dir, "ice-aor-shp.shx")
  )
)

# --- verification

library(validate)

arrests <- arrow::read_feather("data/arrests-latest.feather")

rules <-
  validator(
    n_aors_agrees = sum(in_arrests) == sum(in_aor_sf), # There should be 93 districts - If not, bind may have failed
    n_aors_26 = sum(in_arrests) == 26
  )

arrests |>
  filter(!is.na(apprehension_aor)) |>
  distinct(apprehension_aor) |>
  mutate(in_arrests = 1) |>
  left_join(
    aor_sf |>
      as_tibble() |>
      distinct(area_of_responsibility_name) |>
      mutate(in_aor_sf = 1),
    by = c("apprehension_aor" = "area_of_responsibility_name")
  ) |>
  confront(rules) |>
  summary() |>
  (\(x) {
    if (any(x$fails > 0 | x$errors > 0)) {
      stop(
        "Validation failed: ",
        paste(x$name[x$fails > 0 | x$errors > 0], collapse = ", ")
      )
    } else {
      x
    }
  })()
