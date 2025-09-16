library(tidyverse)
library(sf)
library(tigris)
library(sfarrow)

# aor_labels_df <-
#   tibble::tribble(
#     ~`Area of Responsibility`,
#     ~`AOR Abbreviation`,
#     "Atlanta",
#     "ATL",
#     "Baltimore",
#     "BAL",
#     "Boston",
#     "BOS",
#     "Buffalo",
#     "BUF",
#     "Chicago",
#     "CHI",
#     "Dallas",
#     "DAL",
#     "Denver",
#     "DEN",
#     "Detroit",
#     "DET",
#     "El Paso",
#     "ELP",
#     "Harlingen",
#     "HAL",
#     "Houston",
#     "HOU",
#     "HQ",
#     "HQ",
#     "Los Angeles",
#     "LOS",
#     "Miami",
#     "MIA",
#     "Newark",
#     "NEW",
#     "New Orleans",
#     "NOL",
#     "New York City",
#     "NYC",
#     "Philadelphia",
#     "PHI",
#     "Phoenix",
#     "PHO",
#     "Seattle",
#     "SEA",
#     "San Francisco",
#     "SFR",
#     "Salt Lake City",
#     "SLC",
#     "San Antonio",
#     "SNA",
#     "San Diego",
#     "SND",
#     "St. Paul",
#     "SPM",
#     "Washington",
#     "WAS"
#   )

# county_aor_df <- read_csv(
#   "https://raw.githubusercontent.com/UWCHR/ice-enforce/main/share/hand/county_aor.csv",
#   progress = FALSE
# )

# # Change counties to HAR if in Harlingen AOR
# aor_df <-
#   county_aor_df |>
#   mutate(
#     aor = case_when(
#       name %in%
#         c(
#           "cameron county, texas",
#           "willacy county, texas",
#           "kenedy county, texas",
#           "kleberg county, texas",
#           "nueces county, texas",
#           "san patricio county, texas",
#           "hidalgo county, texas",
#           "brooks county, texas",
#           "starr county, texas",
#           "jim hogg county, texas",
#           "zapata county, texas",
#           "webb county, texas",
#           "duval county, texas",
#           "jim wells county, texas",
#           "aransas county, texas"
#         ) ~
#         "HAL",
#       TRUE ~ aor
#     )
#   ) |>
#   left_join(
#     aor_labels_df |> select(`Area of Responsibility`, `AOR Abbreviation`),
#     by = c("aor" = "AOR Abbreviation")
#   ) |>
#   select(-aor)

# write_csv(aor_df, "data/ice-county-aor-df.csv")

aor_df <- read_csv("data/ice-county-aor-df.csv")

field_office_df <- arrow::read_feather(
  "data/ice-offices.feather"
) |>
  filter(office_category == "field office", office_type == "ERO")

counties_sf <- counties(cb = TRUE, year = 2024, class = "sf", progress = FALSE)

# note valdez-cordova census area is in aor_df but not the counties file because it no longer exists
# the aor file does have the two it was split into, copper river and chugach
aor_sf <-
  counties_sf |>
  left_join(aor_df, by = c("GEOID" = "geoid")) |>
  mutate(
    `Area of Responsibility` = case_when(
      STUSPS %in% c("AK", "HI", "MP", "GU") ~ "San Francisco",
      STUSPS == "VI" ~ "Miami",
      STUSPS == "AS" ~ NA_character_,
      TRUE ~ `Area of Responsibility`
    )
  ) |>
  filter(!is.na(`Area of Responsibility`)) |>
  group_by(`Area of Responsibility`) |>
  summarize(geometry = st_union(geometry), .groups = "drop") |>
  st_cast("MULTIPOLYGON")

sfarrow::st_write_feather(aor_sf, "data/ice-aor-df.feather")

temp_dir <- tempdir()
temp_shp_path <- file.path(temp_dir, "ice-aor-shapefile.shp")
st_write(aor_sf, temp_shp_path, append = FALSE)

# create a zip file with all necessary shapefile components
zip(
  zipfile = "data/ice-aor-shapefile.zip",
  files = c(
    file.path(temp_dir, "ice-aor-shapefile.shp"),
    file.path(temp_dir, "ice-aor-shapefile.dbf"),
    file.path(temp_dir, "ice-aor-shapefile.prj"),
    file.path(temp_dir, "ice-aor-shapefile.shx")
  )
)
