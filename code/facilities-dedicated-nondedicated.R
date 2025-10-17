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

file_metadata <- read_rds(
  "https://github.com/deportationdata/deportationdata.org/raw/refs/heads/main/metadata.rds"
)

detention_management_files <-
  file_metadata |>
  filter(str_starts(path, "ice/dedicated_nondedicated")) |>
  mutate(
    url = glue::glue(
      "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_{id}",
    )
  )

facilities <-
  detention_management_files$url |>
  set_names(detention_management_files$name) |>
  map_dfr(
    ~ {
      # Download to temporary file first
      dl <- tempfile(fileext = ".xlsx")
      download.file(.x, destfile = dl, mode = "wb")

      shts <- excel_sheets(dl)

      file_date <-
        readxl::read_excel(
          dl,
          sheet = 1,
          col_types = "text",
          n_max = 3
        ) |>
        slice(3) |>
        select(1) |>
        # set names so it's date_str as the first column
        set_names("date_str") |>
        mutate(
          file_original_date = str_extract(
            date_str,
            "\\d{1,2}/\\d{1,2}/\\d{4}"
          ) |>
            as.Date(format = "%m/%d/%Y"),
          .keep = "unused"
        )

      df <-
        shts |>
        # filter to sheets with Dedicated in the name
        keep(~ str_detect(.x, regex("dedicated", ignore_case = TRUE))) |>
        set_names() |>
        map_dfr(
          ~ {
            skp <-
              readxl::read_excel(
                dl,
                sheet = .x,
                col_types = "text",
                skip = 0,
                n_max = 10
              ) |>
              rowwise() |>
              summarize(
                na_count = sum(!is.na(c_across(everything())))
              ) |>
              # find the first row with greater than 2 non-NA values using mutate so I can extract the row number
              mutate(row_num = row_number()) |>
              filter(na_count >= 5) |>
              slice(1) |>
              pull(row_num)

            readxl::read_excel(dl, sheet = .x, col_types = "text", skip = skp)
          },
          .id = "sheet_original"
        ) |>
        bind_cols(file_date)

      print(colnames(df))

      df
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  mutate(file_original = basename(file), .keep = "unused") |>
  mutate(
    row_original = row_number()
  ) |>
  relocate(
    file_original,
    file_original_date,
    sheet_original,
    row_original
  )

facilities_df <-
  facilities |>
  slice_max(
    order_by = file_original_date,
    n = 1,
    with_ties = FALSE,
    by = c("name", "city", "state")
  )

arrow::write_feather(
  facilities_df,
  "data/facilities-dedicated-nondedicated.feather"
)
