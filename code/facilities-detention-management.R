library(tidyverse)
library(readxl)

clean_text <- function(str) {
  str |>
    str_to_lower() |>
    str_replace_all(
      "(?<=\\b)([A-Za-z]\\.)+(?=\\b)",
      ~ str_remove_all(.x, "\\.")
    ) |>
    str_replace_all("[[:punct:]]", " ") |>
    str_squish()
}

clean_facility_name <- function(str) {
  str |>
    str_replace_all(c(
      "\\bcty\\b" = "county",
      "\\bdept of corr\\b" = "department of corrections",
      "\\bcnt\\b" = "center",
      "\\busp\\b" = "us penitentiary",
      "\\bdet\\b" = "detention",
      "\\bfed\\b" = "federal",
      "\\bmet\\b" = "metropolitan",
      "\\bjuv\\b" = "juvenile",
      "\\bctr\\b" = "center",
      "\\bco\\b" = "county",
      "\\bfac\\b" = "facility",
      "\\bproc\\b" = "processing",
      "\\bpen\\b" = "penitentiary",
      "\\bcor\\b" = "correctional",
      "\\bcorr\\b" = "correctional",
      "\\bfacilty\\b" = "facility",
      "\\bfclty\\b" = "facility",
      "\\bspc\\b" = "service processing center",
      "\\breg\\b" = "regional",
      "\\bdept\\b" = "department",
      "\\bdf\\b" = "detention facility",
      "\\bpub\\b" = "public",
      "\\bsfty\\b" = "safety",
      "\\bcplx\\b" = "complex",
      "\\bcorrec\\b" = "correctional",
      "\\bcdf\\b" = "contract detention facility",
      "\\brm\\b" = "room",
      "\\bfo\\b" = "field office"
    ))
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

detention_management_files <-
  structure(
    list(
      file = c(
        "FY19-detentionstats.xlsx",
        "FY20-detentionstats.xlsx",
        "FY21_detention-stats_0301.xlsx",
        "FY21_detention-stats_0317.xlsx",
        "FY21_detention-stats_0331.xlsx",
        "FY21_detention-stats_0414.xlsx",
        "FY21_detentionStats04292021.xlsx",
        "FY21_detentionStats05282021.xlsx",
        "FY21_detentionStats060921.xlsx",
        "FY21_detentionStats061121.xlsx",
        "FY21_detentionStats062421.xlsx",
        "FY21_detentionStats07082021.xlsx",
        "FY21_detentionStats07222021.xlsx",
        "FY21_detentionStats08062021.xlsx",
        "FY21_detentionStats08242021.xlsx",
        "FY21_detentionStats09132021.xlsx",
        "FY21_detentionStats09162021.xlsx",
        "FY21_detentionStats10012021.xlsx",
        "FY21_detentionStats10072021.xlsx",
        "FY21_detentionStats10282021.xlsx",
        "FY21_detentionStats11122021.xlsx",
        "FY21_detentionStats210503.xlsx",
        "FY21_detentionStats210513.xlsx",
        "FY21-detentionstats.xlsx",
        "FY22_detentionStats_02162022.xlsx",
        "FY22_detentionStats_03022022.xlsx",
        "FY22_detentionStats_03312022.xlsx",
        "FY22_detentionStats01102022.xlsx",
        "FY22_detentionStats01202022.xlsx",
        "FY22_detentionStats03172022b.xlsx",
        "FY22_detentionStats04132022.xlsx",
        "FY22_detentionStats04282022.xlsx",
        "FY22_detentionStats05042022.xlsx",
        "FY22_detentionStats05242022.xlsx",
        "FY22_detentionStats06092022.xlsx",
        "FY22_detentionStats06162022.xlsx",
        "FY22_detentionStats06172022.xlsx",
        "FY22_detentionStats06232022.xlsx",
        "FY22_detentionStats06272022.xlsx",
        "FY22_detentionStats07082022.xlsx",
        "FY22_detentionStats07212022.xlsx",
        "FY22_detentionStats08042022.xlsx",
        "FY22_detentionStats08092022.xlsx",
        "FY22_detentionStats08192022.xlsx",
        "FY22_detentionStats08312022.xlsx",
        "FY22_detentionStats09152022.xlsx",
        "FY22_detentionStats09292022.xlsx",
        "FY22_detentionStats12292021.xlsx",
        "FY22-detentionStats.xlsx",
        "FY23_detentionStats.xlsx",
        "FY23_detentionStats01092023.xlsx",
        "FY23_detentionStats01192022.xlsx",
        "FY23_detentionStats02032023.xlsx",
        "FY23_detentionStats02282023.xlsx",
        "FY23_detentionStats03032023.xlsx",
        "FY23_detentionStats03312023.xlsx",
        "FY23_detentionStats04122023b.xlsx",
        "FY23_detentionStats04282023.xlsx",
        "FY23_detentionStats05192023.xlsx",
        "FY23_detentionStats05262023.xlsx",
        "FY23_detentionStats05312023.xlsx",
        "FY23_detentionStats06012023.xlsx",
        "FY23_detentionStats06122023.xlsx",
        "FY23_detentionStats07102023.xlsx",
        "FY23_detentionStats07202023.xlsx",
        "FY23_detentionStats08032023.xlsx",
        "FY23_detentionStats08182023.xlsx",
        "FY23_detentionStats08312023.xlsx",
        "FY23_detentionStats09252023.xlsx",
        "FY23_detentionStats09292023.xlsx",
        "FY23_detentionStats10312023.xlsx",
        "FY23_detentionStats11162022.xlsx",
        "FY23_detentionStats112822.xlsx",
        "FY23_detentionStats12062022.xlsx",
        "FY23_detentionStats221207.xlsx",
        "FY23_detentionStats221212.xlsx",
        "FY23_detentionStats221222.xlsx",
        "FY23_detentionStats230316.xlsx",
        "FY24_detentionStats.xlsx",
        "FY24_detentionStats01042024.xlsx",
        "FY24_detentionStats01182024.xlsx",
        "FY24_detentionStats02012024.xlsx",
        "FY24_detentionStats02152024.xlsx",
        "FY24_detentionStats02292024.xlsx",
        "FY24_detentionStats03142024.xlsx",
        "FY24_detentionStats03292024.xlsx",
        "FY24_detentionStats04122024.xlsx",
        "FY24_detentionStats04252024.xlsx",
        "FY24_detentionStats05132024.xlsx",
        "FY24_detentionStats05242024.xlsx",
        "FY24_detentionStats06062024.xlsx",
        "FY24_detentionStats06202024.xlsx",
        "FY24_detentionStats07082024.xlsx",
        "FY24_detentionStats07182024.xlsx",
        "FY24_detentionStats08152024.xlsx",
        "FY24_detentionStats08292024.xlsx",
        "FY24_detentionStats09172024.xlsx",
        "FY24_detentionStats11152023.xlsx",
        "FY24_detentionStats11152024.xlsx",
        "FY24_detentionStats11222023.xlsx",
        "FY24_detentionStats12072023.xlsx",
        "FY24_detentionStats12202023.xlsx",
        "FY25_detentionStats01032025.xlsx",
        "FY25_detentionStats01162025.xlsx",
        "FY25_detentionStats02032025.xlsx",
        "FY25_detentionStats02142025.xlsx",
        "FY25_detentionStats02272025.xlsx",
        "FY25_detentionStats03052025.xlsx",
        "FY25_detentionStats03142025.xlsx",
        "FY25_detentionStats03262025.xlsx",
        "FY25_detentionStats04162025.xlsx",
        "FY25_detentionStats05092025.xlsx",
        "FY25_detentionStats05122025.xlsx",
        "FY25_detentionStats05232025.xlsx",
        "FY25_detentionStats06052025.xlsx",
        "FY25_detentionStats06202025.xlsx",
        "FY25_detentionStats07072025.xlsx",
        "FY25_detentionStats07172025.xlsx",
        "FY25_detentionStats08012025.xlsx",
        "FY25_detentionStats08142025.xlsx",
        "FY25_detentionStats08292025.xlsx",
        "FY25_detentionStats09112025.xlsx",
        "FY25_detentionStats09252025.xlsx",
        "FY25_detentionStats11212024.xlsx",
        "FY25_detentionStats12112024.xlsx",
        "FY25_detentionStats12122024.xlsx",
        "FY25_detentionStats12182024.xlsx"
      ),
      id = c(
        "1836538549260",
        "1836558270675",
        "1940904921171",
        "1940912962867",
        "1940916285575",
        "1940913894081",
        "1940915947115",
        "1940909887031",
        "1940912953557",
        "1940916241709",
        "1940916407415",
        "1940907771615",
        "1940908650485",
        "1940913342084",
        "1940919320598",
        "1940919440819",
        "1940919802973",
        "1940912123545",
        "1940907496257",
        "1940913162043",
        "1940909020231",
        "1940908021660",
        "1940913297574",
        "1836552983586",
        "1940910321525",
        "1940906572276",
        "1940912303490",
        "1940907136043",
        "1940910950158",
        "1940906948837",
        "1940911988538",
        "1940905778014",
        "1940912850610",
        "1940911194243",
        "1940919738187",
        "1940907911462",
        "1940914302649",
        "1940911364962",
        "1940915903684",
        "1940903910232",
        "1940911010158",
        "1940906991764",
        "1940919155877",
        "1940905467769",
        "1940906478053",
        "1940910892078",
        "1940906997367",
        "1940912536107",
        "1836537940671",
        "1836545074923",
        "1940916580593",
        "1940912048442",
        "1940910738664",
        "1940915190064",
        "1940909586300",
        "1940912113859",
        "1940912051015",
        "1940909751864",
        "1940905813740",
        "1940906754538",
        "1940913697446",
        "1940905014976",
        "1940909284347",
        "1940913817260",
        "1940911251765",
        "1940915962929",
        "1940906301076",
        "1940907013367",
        "1940911763830",
        "1940907570429",
        "1940908029365",
        "1940911060682",
        "1940911484515",
        "1940912552762",
        "1940911184737",
        "1940906831784",
        "1940920797250",
        "1940907755224",
        "1836537358917",
        "1941086997347",
        "1941086543044",
        "1941081416603",
        "1941082190380",
        "1941074359174",
        "1941076962190",
        "1941082281029",
        "1941074375974",
        "1941082211453",
        "1941086522189",
        "1941085247212",
        "1941080010085",
        "1941084004584",
        "1941085786880",
        "1941083200417",
        "1941081969293",
        "1941082436598",
        "1941077817551",
        "1940911605485",
        "1941072318562",
        "1940906505313",
        "1940913075897",
        "1940908777252",
        "1836551099839",
        "1836555985237",
        "1836545131055",
        "1836552563416",
        "1836540128756",
        "1836550259001",
        "1836541591004",
        "1836554085594",
        "1836552829382",
        "1875150256066",
        "1875141001701",
        "1875140334740",
        "1886225303775",
        "1901614230787",
        "1919557470456",
        "1932628621335",
        "1942873587614",
        "1955820139164",
        "1984423132721",
        "1984418543821",
        "1998729580163",
        "1836555637335",
        "1836546642190",
        "1836548019897",
        "1836554106108"
      ),
      skip = c(
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        5,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        5,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        3,
        6,
        6,
        5,
        6,
        6,
        6,
        6,
        6,
        6,
        5,
        6,
        6,
        6,
        5,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6,
        6
      )
    ),
    row.names = c(NA, -127L),
    class = "data.frame"
  ) |>
  mutate(
    url = glue::glue(
      "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_{id}",
    )
  )

facilities <-
  detention_management_files$url |>
  set_names(detention_management_files$file) |>
  map2_dfr(
    detention_management_files$skip,
    ~ {
      # Download to temporary file first
      dl <- tempfile(fileext = ".xlsx")
      download.file(.x, destfile = dl, mode = "wb")

      shts <- excel_sheets(dl)
      sht <- shts[which(str_detect(shts, "Facilities"))]

      read_excel(
        dl,
        sheet = sht,
        col_types = "text",
        skip = .y
      )
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  mutate(file = basename(file)) |>
  mutate(row = row_number(), .after = "file", .by = "file")

facilities_df <-
  facilities |>
  filter(!is.na(name) & !is.na(address)) |>
  mutate(
    fy = str_extract(file, regex("(?<=FY)\\d{2}")),
    date_raw = str_extract(file, regex("\\d{8}(?=\\.xlsx$)")) |> mdy(),
    date = if_else(
      is.na(date_raw),
      as.Date(str_c("20", fy, "-12-31")),
      date_raw
    )
  ) |>
  select(-date_raw)

# correct data errors
facilities_df <-
  facilities_df |>
  mutate(
    # a few names have footnotes in them in the excel, remove those numbers
    name = str_remove_all(name, "\\d"),
    # misspelling
    city = case_match(
      city,
      "COTTONWOOD FALL" ~ "COTTONWOOD FALLS",
      "Cottonwood Fall" ~ "COTTONWOOD FALLS",
      .default = city
    ),
    name = case_match(
      name,
      "NORTHWEST ICE PROCESSSING CENTER" ~ "NORTHWEST ICE PROCESSING CENTER",
      "NORTHWEST ICE PROCSESING CENTER" ~ "NORTHWEST ICE PROCESSING CENTER",
      .default = name
    ),
    city = case_match(
      city,
      "BINGHAMPTON" ~ "BINGHAMTON",
      "COTTONWOOD FALL" ~ "COTTONWOOD FALLS",
      "SAULT STE MARIE" ~ "SAULT SAINTE MARIE",
      .default = city
    ),
    address = case_match(
      address,
      "911 PARR BLVD 775 328 3308" ~ "911 PARR BOULEVARD",
      "1001 CENTER WAY" ~ "1001 CENTRE WAY",
      "11866 HASTINGS BRIDGE ROAD" ~ "11866 HASTINGS BRIDGE ROAD P.O. BOX 429",
      "4909 Fm 2826" ~ "4909 Fm (farm To Market) 2826",
      "203 ASPINAL AVE. PO BOX 3236" ~ "203 ASPINALL AVE",
      "3130 OAKLAND ST" ~ "3130 N OAKLAND ST",
      "11866 HASTINGS BRIDGE ROAD P.O. BOX 429" ~ "11866 HASTINGS BRIDGE RD",
      "11866 Hastings Bridge Road P.o. Box 429" ~ "11866 HASTINGS BRIDGE RD",
      "RD. 2, BOX 1" ~ "112 NORTHERN REGIONAL CORRECTIONAL DR", # po box can't be geocoded
      .default = address
    ),
    city = case_when(
      name %in%
        c("RICHWOOD CORRECTIONAL CENTER", "Richwood Correctional Center") &
        city %in% c("Richwood", "RICHWOOD") &
        state == "LA" ~
        "MONROE",
      TRUE ~ city
    ),
    address = case_when(
      address %in%
        c("419 SHOEMAKER ROAD", "419 Shoemaker Road") &
        city %in% c("LOCK HAVEN", "Lock Haven") &
        state == "PA" ~
        "58 PINE MOUNTAIN ROAD",
      TRUE ~ address
    ),
    city = case_when(
      address %in%
        c("58 PINE MOUNTAIN ROAD") &
        city %in% c("LOCK HAVEN", "Lock Haven") &
        state == "PA" ~
        "MCELHATTAN",
      TRUE ~ city
    ),
  )

# facilities |>
#   filter(str_detect(str_to_lower(city), "lock haven")) |>
#   distinct(name, address, city, state)

# facilities |>
#   mutate(across(c(name, address, city, state), clean_text)) |> # remove commas, periods
#   mutate(
#     name = clean_facility_name(name),
#     address = clean_street_address(address)
#   ) |>
#   distinct(name, address, city, state, .keep_all = TRUE) |>
#   mutate(cnt = n(), .by = c(name, city, state)) |>
#   filter(cnt > 1) |>
#   select(1:9, date, cnt) |>
#   arrange(state, name, desc(date)) |> print(n=500)

facilities_df <-
  facilities_df |>
  slice_max(
    order_by = date,
    n = 1,
    with_ties = FALSE,
    by = c("name", "city", "state")
  )

arrow::write_feather(
  facilities_df,
  "data/facilities-detention-management.feather"
)

# facilities <-
#   facilities |>
#   mutate(
#     address = case_match(
#       address,
#       "911 PARR BLVD 775 328 3308" ~ "911 PARR BOULEVARD",
#       "1001 CENTER WAY" ~ "1001 CENTRE WAY",
#       "11866 HASTINGS BRIDGE ROAD" ~ "11866 HASTINGS BRIDGE ROAD P.O. BOX 429",
#       "1040 BERKS RD" ~ "1040 BERKS ROAD",
#       "4909 FM (FARM TO MARKET) 2826" ~ "4909 FM 2826",
#       "203 ASPINAL AVE PO BOX 3236" ~ "203 ASPINALL AVE",
#       "3130 OAKLAND ST" ~ "3130 N OAKLAND ST",
#       .default = address
#     ),
#     across(
#       c(address, city),
#       ~ str_to_title(.x) |> str_replace_all("[,\\.]", " ") |> str_squish()
#     ), # remove commas, periods
#     address = str_replace_all(
#       address,
#       c(
#         "\\bNorth\\b" = "N",
#         "\\bSouth\\b" = "S",
#         "\\bEast\\b" = "E",
#         "\\bWest\\b" = "W"
#       )
#     ),
#     city = case_match(
#       city,
#       "Cottonwood Fall" ~ "Cottonwood Falls",
#       "Sault Ste Marie" ~ "Sault Sainte Marie",
#       # Name == "RICHWOOD CORRECTIONAL CENTER" & City == "RICHWOOD" ~ "MONROE",
#       .default = city
#     )
#   )

# arrange(desc(date)) |>
# mutate(location_num = row_number(), .by = c("name", "state")) |>
# pivot_wider(
#   id_cols = c("name", "state"),
#   names_from = location_num,
#   values_from = c(address, city)
# ) |>
# rename(
#   address = address_1,
#   city = city_1,
#   address_alt = address_2,
#   city_alt = city_2
# )

# map(read_excel, sheet = "Facilities FY25", skip = 6) |>
#   mutate(name_join = clean_facility_name(Name)) |>
#   mutate(facilities_2025_ID = 1:n())
