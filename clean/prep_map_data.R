library(tidyverse)
library(sf)
library(tigris)
library(tidygeocoder)
library(sfarrow)

# Function for shifting a state/territory x, y meters
st_shift <- function(sf_object, x = 0, y = 0) {
  if (inherits(sf_object, "sf")) {
    st_geometry(sf_object) <- st_geometry(sf_object) + c(x, y)
    return(sf_object)
  } else if (inherits(sf_object, "sfc")) {
    return(sf_object + c(x, y))
  }
}

county_aor <- read_csv("https://raw.githubusercontent.com/UWCHR/ice-enforce/main/share/hand/county_aor.csv")

# Change counties to HAR if in Harlingen AOR
har <- c("cameron county, texas", "willacy county, texas", "kenedy county, texas", 
         "kleberg county, texas", "nueces county, texas", "san patricio county, texas",
         "hidalgo county, texas", "brooks county, texas", "starr county, texas",
         "jim hogg county, texas", "zapata county, texas", "webb county, texas",
         "duval county, texas", "jim wells county, texas", "aransas county, texas")
county_aor[county_aor$name %in% har, "aor"] <- "HAR"

# Create a SF object for mapping
counties_sf <- counties(cb = TRUE, year = 2024, class = "sf")

# Separate lower 48 from Alaska, Hawaii, PR, and Guam
mainland <- counties_sf %>%
  filter(!STATEFP %in% c("02", "15", "72", "66", "78", "69"))

mainland <- mainland %>%
  st_transform(9311)

alaska <- counties_sf %>%
  filter(STATEFP == "02") %>%
  # Transform coordinates to meters
  st_transform(9311) %>%
  mutate(
    # Scale by 0.5
    geometry = geometry * 0.5,
    # Shift by x, y meters
    geometry = st_shift(geometry, x = 20000, y = -3750000)
  ) %>% 
  st_set_crs(9311)

hawaii <- counties_sf %>%
  filter(STATEFP == "15") %>%
  st_transform(9311) %>%
  mutate(geometry = st_shift(geometry, x = 3400000, y = -500000)) %>%
  st_set_crs(9311)

pr <- counties_sf %>%
  filter(STATEFP == "72") %>%
  st_transform(9311) %>%
  mutate(
    geometry = geometry * 1.5,
    geometry = st_shift(geometry, x = -2500000, y = 1400000)
  ) %>%
  st_set_crs(9311)

guam <- counties_sf %>%
  filter(STATEFP == "66") %>%
  st_transform(9311) %>%
  mutate(
    geometry = geometry * 3,
    geometry = st_shift(geometry, x = 23250000, y = -13750000)
  ) %>%
  st_set_crs(9311)

vi <- counties_sf %>%
  filter(STATEFP == "78") %>%
  st_transform(9311) %>%
  mutate(
    geometry = geometry * 5,
    geometry = st_shift(geometry, x = -16000000, y = 9750000)
  ) %>%
  st_set_crs(9311)

nmi <- counties_sf %>%
  filter(STATEFP == "69",
         COUNTYFP %in% c("100", "110", "120")) %>%
  st_transform(9311) %>%
  mutate(
    geometry = geometry * 8,
    geometry = st_shift(geometry, x = 67750000, y = -38000000)
  ) %>%
  st_set_crs(9311)

# Recombine data
map_data <- rbind(mainland, alaska, hawaii, pr, guam, vi, nmi)
rm(alaska, hawaii, pr, guam, vi, nmi)

# Merge in AOR
map_data <- merge(map_data, county_aor, by.x = "GEOID", by.y = "geoid", all.x=TRUE)

# Fill in missing AORs
map_data[map_data$STUSPS == "VI", "aor"] <- "MIA"
map_data[map_data$STUSPS %in% c("MP", "GU"), "aor"] <- "SFR"

map_data <- map_data %>%
  # American Samoa has missing AOR
  filter(!is.na(aor)) %>%
  select(STATEFP, COUNTYFP, NAME, STUSPS, aor, geometry)

# st_write(map_data, "ice-aor/ice_aor_map_data.shp", append = FALSE)

sfarrow::st_write_feather(map_data, "outputs/ice_aor_map_data.feather")

# Read in field office info
field_offices <- read_csv("https://raw.githubusercontent.com/UWCHR/ice-enforce/refs/heads/main/share/hand/aor_ero_field_office.csv")

# Convert field office df into sf
field_offices <- st_as_sf(field_offices, coords = c("long", "lat"), crs = 4269)
field_offices <- st_transform(field_offices, st_crs(mainland))

# st_write(field_offices, "outputs/ice_field_offices_geocoded.shp", append = FALSE)

sfarrow::st_write_feather(field_offices, "outputs/ice_field_offices_geocoded.feather")


