library(RSelenium)
library(wdman)
library(netstat)
library(httr)
library(rvest)
library(tidyverse)
library(stringr)
library(dplyr)
library(purrr)
library(webdriver)

# --- scrape FOIA detention facility contracts ---
url <- "https://www.ice.gov/foia/library"

# start RSelenium with Firefox
remDr <- remoteDriver(remoteServerAddr = "localhost", port = 4445L, browserName = "firefox")
remDr$open()

# navigate to URL
remDr$navigate(url)
Sys.sleep(3)

# find dropdown element by its ID
dropdown <- remDr$findElement(using = "id", value = "edit-field-foia-category-target-id")

# click dropdown to reveal options
dropdown$clickElement()
Sys.sleep(1)

# find "Detention Facility Contracts" option and click it
contract_option <- remDr$findElement(using = "xpath", value = "//select[@id='edit-field-foia-category-target-id']/option[contains(text(),'Detention Facility Contracts')]")
contract_option$clickElement()
Sys.sleep(1)

# click Apply button
apply_button <- remDr$findElement(using = "id", value = "edit-submit-foia-request-library-content-")
apply_button$clickElement()
Sys.sleep(5)

# get HTML source after filtering
page_source <- remDr$getPageSource()[[1]]
page <- read_html(page_source)

# parse contracts
contracts <- page |> html_elements(".usa-collection__item")

parse_contracts <- function(contract) {
  day <- contract |>
    html_element(".usa-collection__calendar-date-month") |> 
    html_text(trim = TRUE)
  
  year <- contract |>
    html_element(".usa-collection__calendar-date-day") |> 
    html_text(trim = TRUE)
  
  title <- contract |> 
    html_element(".usa-collection__heading") |> 
    html_text(trim = TRUE)
  
  link <- contract |> 
    html_element(".usa-collection__heading a") |> 
    html_attr("href")
  
  tibble(
    date = paste(day, year, sep = ", "),
    title = title,
    facility = title |>
      str_extract("^[^|]+") |> 
      str_remove("\\s*[-–]\\s*[^,]+,\\s*[A-Z]{2}$") |> 
      str_trim(),
    city = str_trim(str_extract(title, "(?:-|–)\\s*([^,]+)"), side = "both"),
    state = str_extract(title, "\\b[A-Z]{2}\\b"),
    agency = "Contract",
    link = link
  )
}

all_contracts <- tibble()
page_number <- 1

repeat {
  message(paste("Scraping page", page_number))
  
  # get current HTML
  page_source <- remDr$getPageSource()[[1]]
  page <- read_html(page_source)
  
  # extract all contract elements
  contracts <- page |> html_elements(".usa-collection__item")
  
  # parse and bind
  parsed <- map_dfr(contracts, parse_contracts)
  all_contracts <- bind_rows(all_contracts, parsed)
  
  # look for "Next" button
  next_button <- tryCatch({
    remDr$findElement(using = "xpath", value = "//a[contains(@aria-label, 'Next page')]")
  }, error = function(e) NULL)
  
  if (is.null(next_button)) {
    message("no more pages found")
    break
  }
  
  # click next page and wait
  next_button$clickElement()
  Sys.sleep(4)
  page_number <- page_number + 1
}

remDr$close()

all_contracts <- all_contracts |> 
  mutate(date = mdy(date)) |> 
  mutate(
    city = ifelse(is.na(state), NA, city),
    city = str_trim(str_extract(city, "[^-|–]+$"))
  )

# --- scrape FOIA detention facility compliance inspections ---
url <- "https://www.ice.gov/foia/library"

# start RSelenium with Firefox
remDr <- remoteDriver(remoteServerAddr = "localhost", port = 4445L, browserName = "firefox")
remDr$open()

# navigate to URL
remDr$navigate(url)
Sys.sleep(3)

# find dropdown element by its ID
dropdown <- remDr$findElement(using = "id", value = "edit-field-foia-category-target-id")

# click dropdown to reveal options
dropdown$clickElement()
Sys.sleep(1)

# find "Office of Detention Oversight - Detention Facility Compliance Inspections" option and click it
inspection_option <- remDr$findElement(using = "xpath", value = "//select[@id='edit-field-foia-category-target-id']/option[contains(text(),'Office of Detention Oversight')]")
inspection_option$clickElement()
Sys.sleep(1)

# click Apply button
apply_button <- remDr$findElement(using = "id", value = "edit-submit-foia-request-library-content-")
apply_button$clickElement()
Sys.sleep(5)

# get HTML source after filtering
page_source <- remDr$getPageSource()[[1]]
page <- read_html(page_source)

# parse inspections
inspections <- page |> html_elements(".usa-collection__item")

parse_inspections <- function(inspection) {
  title <- inspection |> 
    html_element(".usa-collection__heading") |> 
    html_text(trim = TRUE)
  
  link <- inspection |> 
    html_element(".usa-collection__heading a") |> 
    html_attr("href")
  
  tibble(
    date = title |>
      str_extract("[A-Z][a-z]{2,}\\.?\\s+\\d{1,2}(?:(?:-\\d{1,2})?)?,\\s*\\d{4}") |>
      str_replace("-\\d{1,2}", ""),
    title = title,
    facility = title |>
      str_extract("^[^,]+") |>
      str_remove("^\\d{4}\\s+"),
    city = str_extract(title, "(?<=,\\s)[^,]+(?=,)"),
    state = str_extract(title, "(?<=,\\s)[A-Z]{2}(?=\\s*-)"),
    agency = "Compliance Inspection",
    link = link
  )
}

all_inspections <- tibble()
page_number <- 1

repeat {
  message(paste("Scraping page", page_number))
  
  # get current HTML
  page_source <- remDr$getPageSource()[[1]]
  page <- read_html(page_source)
  
  # extract all contract elements
  inspections <- page |> html_elements(".usa-collection__item")
  
  # parse and bind
  parsed <- map_dfr(inspections, parse_inspections)
  all_inspections <- bind_rows(all_inspections, parsed)
  
  # look for "Next" button
  next_button <- tryCatch({
    remDr$findElement(using = "xpath", value = "//a[contains(@aria-label, 'Next page')]")
  }, error = function(e) NULL)
  
  if (is.null(next_button)) {
    message("no more pages found")
    break
  }
  
  # click next page and wait
  next_button$clickElement()
  Sys.sleep(4)
  page_number <- page_number + 1
}

remDr$close()

all_inspections <- all_inspections |> 
  mutate(
    date = mdy(date),
    city = ifelse(is.na(state), NA, city)
  )

contracts_and_inspections <- bind_rows(all_contracts, all_inspections)

arrow::write_feather(contracts_and_inspections, "data/contracts-and-inspections.feather")
