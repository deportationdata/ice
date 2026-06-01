suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
})

df <- read_parquet("data/detention-stints-latest.parquet")
setDT(df)
setnames(df, "unique_identifier", "anonymized_identifier")

df[, row_idx := .I]

stage1 <- copy(df[!is.na(anonymized_identifier)])
stage1[,
  initial_bond_set_amount := min(initial_bond_set_amount, na.rm = TRUE),
  by = stint_ID
]
stage1[
  is.infinite(initial_bond_set_amount),
  initial_bond_set_amount := NA_real_
]

stage1_dedup_cols <- setdiff(
  names(stage1),
  c(
    "file_original", "sheet_original", "row_original", "row_idx",
    "city", "state", "county",
    "likely_duplicate"
  )
)
stage1_unique <- unique(stage1, by = stage1_dedup_cols)
stage1_kept <- stage1_unique$row_idx

stage1_unique[, stint_date := as.Date(book_in_date_time)]
stage2_unique <- stage1_unique[,
  .SD[.N],
  by = .(anonymized_identifier, detention_facility_code, stint_date, stay_ID)
]
stage2_kept <- stage2_unique$row_idx

df[,
  `:=`(
    likely_duplicate_bond = !is.na(anonymized_identifier) &
      !row_idx %in% stage1_kept,
    likely_duplicate_sameday = !is.na(anonymized_identifier) &
      row_idx %in% stage1_kept &
      !row_idx %in% stage2_kept,
    keep_row = is.na(anonymized_identifier) | row_idx %in% stage2_kept
  )
]

cat("\n=== Row counts ===\n")
cat(sprintf("Total rows:                 %d\n", nrow(df)))
cat(sprintf("likely_duplicate_bond=TRUE: %d\n", sum(df$likely_duplicate_bond)))
cat(sprintf("likely_duplicate_sameday=T: %d\n", sum(df$likely_duplicate_sameday)))
cat(sprintf("keep_row=TRUE:              %d\n", sum(df$keep_row)))
cat(sprintf("NA-ID rows:                 %d\n", sum(is.na(df$anonymized_identifier))))

cat("\n=== Sanity: flag combinations (should be mutually exclusive for non-NA-ID) ===\n")
print(df[!is.na(anonymized_identifier),
  .N,
  by = .(likely_duplicate_bond, likely_duplicate_sameday, keep_row)
])

cat("\n=== NA-ID rows flag values (should be bond=F, sameday=F, keep=T) ===\n")
print(df[is.na(anonymized_identifier),
  .N,
  by = .(likely_duplicate_bond, likely_duplicate_sameday, keep_row)
])

cat("\n=== Compare to existing likely_duplicate ===\n")
cat("Old likely_duplicate counts:\n")
print(df[, .N, by = likely_duplicate])

cat("\nCross-tab (old) likely_duplicate vs (new) keep_row:\n")
print(df[, .N, by = .(likely_duplicate, keep_row)])
