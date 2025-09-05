library(httr)
library(rvest)
library(tidyverse)
library(stringr)
library(dplyr)

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

# function to scrape field offices
scrape_field_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))
  
  # get content for each field office
  field_offices <- page %>% html_elements(".grid__content")
  
  # create empty vectors
  office_type <- c()
  address_line_1 <- c()
  address_line_2 <- c()
  city <- c()
  state <- c()
  zip <- c()
  
  # attaching values from field offices
  for (field_office in field_offices) {
    office_type <- c(office_type, field_office |> html_element(".field-content") |> html_text(trim = TRUE))
    address_line_1 <- c(address_line_1, field_office |> html_element(".address-line1") |> html_text(trim = TRUE))
    address_line_2 <- c(address_line_2, field_office |> html_element(".address-line2") |> html_text(trim = TRUE))
    city <- c(city, field_office |> html_element(".locality") |> html_text(trim = TRUE))
    state <- c(state, field_office |> html_element(".administrative-area") |> html_text(trim = TRUE))
    zip <- c(zip, field_office |> html_element(".postal-code") |> html_text(trim = TRUE))
    }
  
  # create dataframe
  field_office_df <- data.frame(
    office_type = clean_text(sub(".*-\\s*", "", office_type)),
    address = clean_street_address(clean_text(sub("^[^0-9]*", "", ifelse(is.na(address_line_2), address_line_1, paste(address_line_1, address_line_2))))),
    city = clean_text(city),
    state = clean_text(state),
    zip = substr(zip, 1, 5),
    zip_4 = zip,
    stringsAsFactors = FALSE
  )
  
  return(field_office_df)
}

# function to scrape sub-offices
scrape_sub_offices <- function(url) {
  results <- GET(url)
  page <- read_html(content(results, as = "text"))
  
  # get content for each field office
  sub_offices <- page %>% html_elements(".grid__content")
  
  # create empty vectors
  main_office <- c()
  address_line_1 <- c()
  address_line_2 <- c()
  city <- c()
  state <- c()
  zip <- c()
  
  # attaching values from field offices
  for (sub_office in sub_offices) {
    main_office <- c(main_office, sub_office |> html_element(".views-field.views-field-field-field-office-name") |> html_text(trim = TRUE))
    address_line_1 <- c(address_line_1, sub_office |> html_element(".address-line1") |> html_text(trim = TRUE))
    address_line_2 <- c(address_line_2, sub_office |> html_element(".address-line2") |> html_text(trim = TRUE))
    city <- c(city, sub_office |> html_element(".locality") |> html_text(trim = TRUE))
    state <- c(state, sub_office |> html_element(".administrative-area") |> html_text(trim = TRUE))
    zip <- c(zip, sub_office |> html_element(".postal-code") |> html_text(trim = TRUE))
  }
  
  # create dataframe
  sub_office_df <- data.frame(
    main_office = clean_text(main_office),
    address = clean_street_address(clean_text(sub("^[^0-9]*", "", ifelse(is.na(address_line_2), address_line_1, paste(address_line_1, address_line_2))))),
    city = clean_text(city),
    state = clean_text(state),
    zip = substr(zip, 1, 5),
    stringsAsFactors = FALSE
  )
  
  return(sub_office_df)
}

# scrape both types of offices
field_offices <- rbind(
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=0"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=1"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=2"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=3"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=4"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=5"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=6"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=7"),
  scrape_field_offices("https://www.ice.gov/contact/field-offices?page=8")
)

sub_offices <- rbind(
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=0"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=1"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=2"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=3"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=4"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=5"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=6"),
  scrape_sub_offices("https://www.ice.gov/contact/check-in?page=7")
)

# add office type column
field_offices$office_category <- "field office"
sub_offices$office_category <- "sub-office"

# add missing columns to make them compatible
field_offices$main_office <- NA
sub_offices$office_type <- NA
sub_offices$zip_4 <- NA

# reorder columns to match
field_offices <- field_offices %>% 
  select(office_category, office_type, main_office, address, city, state, zip, zip_4)

sub_offices <- sub_offices %>% 
  select(office_category, office_type, main_office, address, city, state, zip, zip_4)

# combine into one dataframe
all_offices <- rbind(field_offices, sub_offices)
