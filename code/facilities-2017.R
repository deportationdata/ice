library(tidyverse)
library(readxl)

f <- "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_1836538055645"
dl <- tempfile(fileext = ".xlsx")
download.file(f, destfile = dl, mode = "wb")

# Download to temp file and read in 2017 data
facilities_2017 <-
  readxl::read_excel(
    dl,
    skip = 8,
    sheet = 2
  ) |>
  janitor::clean_names() |>
  mutate(row = row_number(), .before = 0) |>
  filter(detloc != "Redacted") |>
  readr::type_convert()
# many warnings for redacted rows

# correct data errors (or updated addresses)
facilities_2017 <-
  facilities_2017 |>
  mutate(
    address = case_when(
      address == "419 SHOEMAKER ROAD" & city == "LOCK HAVEN" & state == "PA" ~
        "58 PINE MOUNTAIN ROAD",
      TRUE ~ address
    ),
    city = case_when(
      address == "58 PINE MOUNTAIN ROAD" &
        city == "LOCK HAVEN" &
        state == "PA" ~
        "MCELHATTAN",
      TRUE ~ city
    ),
  )

arrow::write_feather(
  facilities_2017,
  "data/facilities-2017.feather"
)
