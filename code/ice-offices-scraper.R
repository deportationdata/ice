library(httr)
library(rvest)
library(tidyverse)
library(stringr)
library(dplyr)
library(purrr)
library(tidygeocoder)

# functions to scrape field offices and sub-offices
scrape_field_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))
  
  # get content for each field office
  field_offices <- page |> html_elements(".grid__content")
  
  parse_field_office <- function(field_office) {
    area <- field_office |>
      html_element(".views-field.views-field-body .field-content") |>
      html_text(trim = TRUE)
    office_type <- field_office |>
      html_element(".field-content") |>
      html_text(trim = TRUE)
    address_line_1 <- field_office |>
      html_element(".address-line1") |>
      html_text(trim = TRUE)
    address_line_2 <- field_office |>
      html_element(".address-line2") |>
      html_text(trim = TRUE)
    city <- field_office |> html_element(".locality") |> html_text(trim = TRUE)
    state <- field_office |>
      html_element(".administrative-area") |>
      html_text(trim = TRUE)
    zip <- field_office |>
      html_element(".postal-code") |>
      html_text(trim = TRUE)
    
    # clean area
    area <- if (
      !is.na(area) &&
      str_detect(area, regex("Area of Responsibility:", ignore_case = TRUE))
    ) {
      str_match(
        area,
        regex(
          "Area of Responsibility:\\s*(.*?)(?:\\s*Email:|$)",
          dotall = TRUE,
          ignore_case = TRUE
        )
      )[, 2] |>
        str_squish()
    } else {
      NA
    }
    
    tibble(
      area = area,
      office_name = str_remove(office_type, " - .*"),
      office_type = str_replace(office_type, ".*-\\s*", ""),
      address = str_replace(
        ifelse(
          is.na(address_line_2),
          address_line_1,
          paste(address_line_1, address_line_2)
        ),
        "^[^0-9]*",
        ""
      ),
      city = city,
      state = state,
      zip_4 = zip,
      zip = str_extract(zip, "^\\d{5}")
    )
  }
  
  # bind into one tibble
  field_office_df <- map_dfr(field_offices, parse_field_office)
  
  return(field_office_df)
}

scrape_sub_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))
  
  # get content for each sub-office
  sub_offices <- page |> html_elements(".grid__content")
  
  parse_sub_office <- function(sub_office) {
    area <- sub_office |>
      html_element(".views-field.views-field-body .field-content") |>
      html_text(trim = TRUE)
    main_office <- sub_office |>
      html_element(".views-field.views-field-field-field-office-name") |>
      html_text(trim = TRUE)
    address_line_1 <- sub_office |>
      html_element(".address-line1") |>
      html_text(trim = TRUE)
    address_line_2 <- sub_office |>
      html_element(".address-line2") |>
      html_text(trim = TRUE)
    city <- sub_office |> html_element(".locality") |> html_text(trim = TRUE)
    state <- sub_office |>
      html_element(".administrative-area") |>
      html_text(trim = TRUE)
    zip <- sub_office |> html_element(".postal-code") |> html_text(trim = TRUE)
    
    # clean area
    area <- if (
      !is.na(area) &&
      str_detect(area, regex("Area Coverage:", ignore_case = TRUE))
    ) {
      str_match(
        area,
        regex(
          "Area Coverage:\\s*(.*?)(?:\\s*Appointment Times:|$)",
          dotall = TRUE,
          ignore_case = TRUE
        )
      )[, 2] |>
        str_squish()
    } else {
      NA
    }
    
    tibble(
      area = area,
      main_office = main_office,
      address = str_replace(
        ifelse(
          is.na(address_line_2),
          address_line_1,
          paste(address_line_1, address_line_2)
        ),
        "^[^0-9]*",
        ""
      ),
      city = city,
      state = state,
      zip_4 = zip,
      zip = str_extract(zip, "^\\d{5}")
    )
  }
  
  # bind into one tibble
  sub_office_df <- map_dfr(sub_offices, parse_sub_office)
  
  return(sub_office_df)
}

# number of pages helper function
get_num_pages <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))
  
  pages <- page |>
    html_element(".usa-pagination") |>
    html_text(trim = TRUE)
  
  page_nums <- str_extract_all(pages, "\\d+") |>
    unlist() |>
    as.integer()
  
  if (length(page_nums) == 0) {
    return(1)
  } else {
    return(max(page_nums))
  }
}

# get total number of pages
n_field_pages <- get_num_pages("https://www.ice.gov/contact/field-offices")
n_sub_pages <- get_num_pages("https://www.ice.gov/contact/check-in")

# scrape both types of offices
field_offices <- map_dfr(0:(n_field_pages - 1), function(i) {
  url <- paste0("https://www.ice.gov/contact/field-offices?page=", i)
  scrape_field_offices(url)
})

sub_offices <- map_dfr(0:(n_sub_pages - 1), function(i) {
  url <- paste0("https://www.ice.gov/contact/check-in?page=", i)
  scrape_sub_offices(url)
})

# combine into one dataframe
all_offices <-
  bind_rows(
    "Field office" = field_offices,
    "Sub-office" = sub_offices,
    .id = "office_category"
  ) |>
  select(
    office_category,
    office_name,
    office_type,
    main_office,
    address,
    city,
    state,
    zip,
    zip_4,
    area
  ) |>
  mutate(address_full = str_c(address, ", ", city, ", ", state, " ", zip_4))

geocoded_census <-
  all_offices |>
  geocode(
    address = address_full,
    method = "census",
    lat = latitude_census,
    long = longitude_census
  )

not_geocoded_census <-
  all_offices |>
  anti_join(
    geocoded_census |> filter(!is.na(latitude_census)),
    by = c("office_category", "office_name", "office_type", "main_office", "address", "city", "state", "zip", "zip_4", "area", "address_full")
  )

Sys.setenv(GOOGLEGEOCODE_API_KEY = "AIzaSyBEfZio8zgA7S7_irwkcGkNMFKyo0_gYBM")

geocoded_google <-
  not_geocoded_census |>
  geocode(
    address = address_full,
    method = "google",
    lat = latitude_google,
    long = longitude_google
  )

not_geocoded_google <-
  not_geocoded_census |>
  anti_join(
    geocoded_google |> filter(!is.na(latitude_google)),
    by = c("office_category", "office_name", "office_type", "main_office", "address", "city", "state", "zip", "zip_4", "area", "address_full")
  )

geocoded_osm <-
  not_geocoded_google |>
  geocode(
    address = address_full,
    method = "osm",
    lat = latitude_osm,
    long = longitude_osm
  )

all_geocoded <-
  bind_rows(
    "census" = geocoded_census |>
      select(
        office_category:area,
        address_full,
        latitude = latitude_census,
        longitude = longitude_census
      ),
    "google" = geocoded_google |>
      select(
        office_category:area,
        address_full,
        latitude = latitude_google,
        longitude = longitude_google
      ),
    "osm" = geocoded_osm |>
      select(
        office_category:area,
        address_full,
        latitude = latitude_osm,
        longitude = longitude_osm
      ),
    .id = "geocode_source"
  ) |>
  filter(!is.na(latitude))

# ungeocoded addresses
ungeocoded_addresses <-
  not_geocoded_google |>
  anti_join(
    geocoded_osm |> filter(!is.na(latitude_osm)),
    by = c("office_category", "office_type", "main_office", "address", "city", "state", "zip", "zip_4", "area", "address_full")
  )

arrow::write_feather(all_geocoded, "data/ice-offices.feather")
