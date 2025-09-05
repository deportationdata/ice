library(httr)
library(rvest)
library(tidyverse)
library(stringr)
library(dplyr)
library(purrr)

# cleaning functions
clean_text <- function(str) {
  str |>
    str_to_lower() |>
    str_replace_all("(?<=\\b)([A-Za-z]\\.)+(?=\\b)", ~ str_remove_all(.x, "\\.")) |>
    str_replace_all("[[:punct:]]", " ") |>
    str_squish()
}

clean_street_address <- function(str) {
  str |>
    str_replace_all(c(
      "\\bstreet\\b" = "st",
      "\\bavenue\\b" = "ave",
      "\\bboulevard\\b" = "blvd",
      "\\bdrive\\b" = "dr",
      "\\broad\\b" = "rd",
      "\\bhighway\\b" = "hwy",
      "\\bplace\\b" = "pl",
      "\\blane\\b" = "ln",
      "\\bsquare\\b" = "sq",
      "\\bterrace\\b" = "ter",
      "\\bparkway\\b" = "pkwy",
      "\\bcourt\\b" = "ct",
      "\\bnorth\\b" = "n",
      "\\bsouth\\b" = "s",
      "\\beast\\b" = "e",
      "\\bwest\\b" = "w",
      "\\bfort\\b" = "ft"
    ))
}

# functions to scrape field offices and sub-offices
scrape_field_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))
  
  # get content for each field office
  field_offices <- page |> html_elements(".grid__content")
  
  parse_field_office <- function(field_office) {
    office_type <- field_office |> html_element(".field-content") |> html_text(trim = TRUE)
    address_line_1 <- field_office |> html_element(".address-line1") |> html_text(trim = TRUE)
    address_line_2 <- field_office |> html_element(".address-line2") |> html_text(trim = TRUE)
    city          <- field_office |> html_element(".locality") |> html_text(trim = TRUE)
    state         <- field_office |> html_element(".administrative-area") |> html_text(trim = TRUE)
    zip           <- field_office |> html_element(".postal-code") |> html_text(trim = TRUE)
    
    tibble(
      office_type = clean_text(office_type),
      address = clean_street_address(clean_text(str_replace(ifelse(is.na(address_line_2), address_line_1, paste(address_line_1, address_line_2)), "^[^0-9]*", ""))),
      city = clean_text(city),
      state = clean_text(state),
      zip = substr(zip, 1, 5),
      zip_4 = zip
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
    main_office <- sub_office |> html_element(".views-field.views-field-field-field-office-name") |> html_text(trim = TRUE)
    address_line_1 <- sub_office |> html_element(".address-line1") |> html_text(trim = TRUE)
    address_line_2 <- sub_office |> html_element(".address-line2") |> html_text(trim = TRUE)
    city <- sub_office |> html_element(".locality") |> html_text(trim = TRUE)
    state <- sub_office |> html_element(".administrative-area") |> html_text(trim = TRUE)
    zip <- sub_office |> html_element(".postal-code") |> html_text(trim = TRUE)
    
    tibble(
      main_office = clean_text(main_office),
      address = clean_street_address(clean_text(str_replace(ifelse(is.na(address_line_2), address_line_1, paste(address_line_1, address_line_2)), "^[^0-9]*", ""))),
      city = clean_text(city),
      state = clean_text(state),
      zip = substr(zip, 1, 5)
    )
  }
  
  # bind into one tibble
  sub_office_df <- map_dfr(sub_offices, parse_sub_office)
  
  return(sub_office_df)
}

# scrape both types of offices
field_offices <- map_dfr(0:8, function(i) {
  url <- paste0("https://www.ice.gov/contact/field-offices?page=", i)
  scrape_field_offices(url)
})

sub_offices <- map_dfr(0:7, function(i) {
  url <- paste0("https://www.ice.gov/contact/check-in?page=", i)
  scrape_sub_offices(url)
})

# add office type column
field_offices <- field_offices |> 
  mutate(office_category = "field office")

sub_offices <- sub_offices |> 
  mutate(office_category = "sub-office")

# combine into one dataframe
all_offices <- bind_rows(field_offices, sub_offices)
all_offices <- all_offices |>
  select(office_category, office_type, main_office, address, city, state, zip, zip_4)

arrow::write_feather(all_offices, "data/ice_offices.feather")
