library(httr)
library(rvest)
library(tidyverse)
library(stringr)
library(dplyr)
library(purrr)

# functions to scrape facilities
scrape_facilities <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))

  # get content for each facility
  facilities <- page |> html_elements(".grid__content")

  parse_facilities <- function(facility) {
    name <- facility |>
      html_element(".views-field.views-field-title") |>
      html_text(trim = TRUE)
    field_office <- facility |>
      html_element(
        ".views-field.views-field-field-field-office-name .field-content"
      ) |>
      html_text(trim = TRUE)
    address_line_1 <- facility |>
      html_element(".address-line1") |>
      html_text(trim = TRUE)
    address_line_2 <- facility |>
      html_element(".address-line2") |>
      html_text(trim = TRUE)
    city <- facility |>
      html_element(".locality") |>
      html_text(trim = TRUE)
    state <- facility |>
      html_element(".administrative-area") |>
      html_text(trim = TRUE)
    zip <- facility |>
      html_element(".postal-code") |>
      html_text(trim = TRUE)

    tibble(
      name = name,
      field_office = field_office,
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
  facilities_df <- map_dfr(facilities, parse_facilities)

  return(facilities_df)
}

# number of pages helper function
get_num_pages <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))

  pages <- page |>
    html_element(".usa-pagination__list.js-pager__items") |>
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
n_facility_pages <- get_num_pages("https://www.ice.gov/detention-facilities")

# scrape facilities
ice_facilities <- map_dfr(0:(n_facility_pages - 1), function(i) {
  url <- paste0("https://www.ice.gov/detention-facilities?page=", i)
  scrape_facilities(url)
})
