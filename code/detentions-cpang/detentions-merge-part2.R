rm(list=ls())
gc()

library(arrow)
library(data.table)
library(dplyr)

df673_old_cols <- c("Book_Out_Date_Time", "Most_Serious_Conviction_Date", "Most_Serious_Sentence_Months", "Most_Serious_Sentence_Years", "Release_Reason", "Alien_Number_Unique_Identifier", "Book_in_Date_And_Time")
df673_new_cols <- c("Detention_Book_Out_Date_Time", "MSC_Conviction_Date", "MSC_Sentence_Months", "MSC_Sentence_Years", "Detention_Release_Reason", "Unique_Identifier", "Detention_Book_In_Date_Time")

df2451_old_cols <- c("Eid_Dt_Civ_Id")
df2451_new_cols <- c("EID_Civilian_Id")

# ---- helper: rename on Arrow Table / RecordBatchReader / data.frame safely ----
rename_any <- function(x, old, new) {
  keep <- old %in% names(x)
  if (any(keep)) {
    # dplyr::rename wants: new_name = old_name
    mapping <- stats::setNames(old[keep], new[keep])  # names are NEW, values are OLD
    x <- dplyr::rename(x, !!!mapping)
  }
  x
}

# ---- paths ----
path1 <- "code/detentions-cpang/temp_df2451.feather"
path2 <- "code/detentions-cpang/temp_df673.feather"
out_dir <- "code/detentions-cpang/merged_parquet"  # folder output

# ---- read as Arrow (NOT in-memory data.frames) ----
t1 <- arrow::read_feather(path1, as_data_frame = FALSE)
t2 <- arrow::read_feather(path2, as_data_frame = FALSE)

# ---- rename (lazy) ----
t1 <- rename_any(t1, df2451_old_cols, df2451_new_cols)
t2 <- rename_any(t2, df673_old_cols,  df673_new_cols)

# ---- optional: make shared columns the same type (string) BEFORE binding ----
common <- intersect(names(t1), names(t2))
if (length(common) > 0) {
  t1 <- t1 %>% mutate(across(all_of(common), ~ as.character(.x)))
  t2 <- t2 %>% mutate(across(all_of(common), ~ as.character(.x)))
}

# ---- bind in Arrow and write to disk (no huge in-R object) ----
t1_tbl <- dplyr::compute(t1)
t2_tbl <- dplyr::compute(t2)

merged_tbl <- arrow::concat_tables(t1_tbl, t2_tbl, unify_schemas = TRUE)
if (dir.exists(out_dir)) unlink(out_dir, recursive = TRUE, force = TRUE)

# saves file to part-0.parquet
arrow::write_dataset(merged_tbl, out_dir, format = "parquet")
