get_weekly_counts <- function(df, date_col_name){
  df <- df |>
    rename(Detention_Book_In_Date = date_col_name)

  weekly_counts_by_src <- df |>
  filter(!is.na(Detention_Book_In_Date)) |>
  mutate(
    Detention_Book_In_Date = as.Date(Detention_Book_In_Date),
    week_start = floor_date(Detention_Book_In_Date, unit = "week", week_start = 7)
  ) |>
  group_by(source_file, week_start) |>
  summarise(
    n = n(),
    .groups = "drop"
  ) |>
  arrange(source_file, week_start)
  
  return(weekly_counts_by_src)
}
