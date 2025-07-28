guess_types_across_sheets <- function(path, sheets = NULL, skip, guess_max = 16384) {
  sheets <- sheets %||% readxl::excel_sheets(path)

  # per-sheet helper
  sheet_guess <- function(sh, skip) {
    # let readxl do its compiled guessing
    df <- readxl::read_excel(
      path, 
      sheet = sh,
      skip = skip - 1,
      col_types = NULL,
      guess_max = guess_max,
      trim_ws = FALSE
    )

    # map R classes back to readxl type labels
    vapply(df, function(col) {
      if (inherits(col, c("POSIXct", "POSIXlt", "Date"))) "date"
      else if (is.numeric(col))         "numeric"
      else if (is.logical(col))         "logical"
      else if (is.character(col))       "text"
      else                              "list"   # readxl's fallback
    }, character(1))
  }

  # Get column types for each sheet
  per_sheet <- map2(sheets, skip, ~ {
    if (is.na(.y)) return(NULL)
    sheet_guess(.x, .y) 
  })
  
  # Remove NULL entries and set names
  per_sheet <- per_sheet[!sapply(per_sheet, is.null)]
  names(per_sheet) <- sheets[!is.na(skip)]

  # Find all unique column names across sheets
  all_columns <- unique(unlist(map(per_sheet, names)))

  # Type precedence for choosing the most general type
  precedence <- c(blank = 1L, logical = 2L, numeric = 3L,
                  date = 4L, text = 5L, list = 6L)
  
  # go through each column and determine the type by looking across all sheets and choosing based on precedence
  col_types <- map(all_columns, function(col_name) {
    # Get types for this column across all sheets
    # types <- map_chr(per_sheet, ~ .x[[col_name]] %||% "blank")

    per_sheet_has_col <-
      per_sheet |>
      keep(~ col_name %in% names(.x))
    
    # names(per_sheet_has_col)   # shows which sheets survived
    
    types <-
      per_sheet_has_col |>
      map_chr(col_name)          # map_chr automatically extracts by name

    # Determine the most general type based on precedence
    type_order <- precedence[types]
    most_general_type <- names(precedence[names(which(type_order == max(type_order, na.rm = TRUE))[1])])
    
    # Return the most general type for this column
    return(most_general_type)
  })
  
  # Set names for the column types
  names(col_types) <- all_columns

  # now create a list that is of length sheets with the column types for each sheet
  col_types <- map(sheets, function(sheet) {
    # Get the types for this sheet
    types_for_sheet <- col_types[names(per_sheet[[sheet]]) %||% character(0)]
    
    # If the sheet doesn't have any columns, return an empty list
    if (is.null(types_for_sheet)) return(list())
    
    # Return the types for this sheet
    return(types_for_sheet)
  })

  # Return the column type specifications
  return(col_types)
}
