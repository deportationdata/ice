# --- Clear Memory ---
rm(list=ls())

# --- Packages ---
library(dplyr)
library(tibble)
library(stringdist)
library(stringr)

# --- Source Functions ---
source("code/functions/inspect_columns.R")
source("code/functions/merge_two_df.R")

# --- Read in Combined Data ---

df1 <- read_feather("data/ice-raw/arrests-selected/2022-ICFO-22955_combined.feather")
df2 <- read_feather("data/ice-raw/arrests-selected/2023_ICFO_42034_combined.feather")
df3 <- read_feather("data/ice-raw/arrests-selected/120125_combined.feather")
df4 <- read_feather("data/ice-raw/arrests-selected/uwchr_combined.feather")

# Step 0. Get Shared column matrix to determine which datasets to compare FIRST 
df_list <- list(df1 = df1, 
                df2 = df2, 
                df3 = df3, 
                df4 = df4)
shared_col_matrix <- get_shared_cols(df_list)
print(shared_col_matrix)

# Step 1 (a). Inspect the columns that are shared and unique between datasets
venn_1_2b <- inspect_columns(names(df1), names(df3)) 
# --- There appears to be 13 overlapping columns between df1 and df2 

# Step 1 (b). Flag near-matches between datasets
near_matches_1_2 <- flag_near_matches(names(df3), names(df1), max_dist = 2)

# Step 2. Rename the columns that are probably mergeable (i.e., similar names for the same field)
# add Apprehension_Date column 

df1 <- df1 %>%
  mutate(
    Apprehension_Date = str_sub(Apprehension_Date_And_Time, 1, 10)
  )

df1_cols_old <- c("Final_Order_Yes_No.Blank",
                      "Sequence_Number.Unique_Identifier",
                      "County",
                      "State",
                      "Apprehension_Final_Program",
                      "ApprehensionFinal_Program_Group",
                      "Numeric_Birth_Year")

df1_cols_new <- c("Final_Order_Yes_No",
                      "Unique_Identifier",
                      "Apprehension_County",
                      "Apprehension_State",
                      "Final_Program",
                      "Final_Program_Group",
                      "Birth_Year")
df3_cols_old <- c("Apprehension_Site_Landmark")
df3_cols_new <- c("Apprehension_Landmark")

# Step 3. Merge the datasets
merge_1_3_out <- merge_dfs(df1, df3,
                        df1_cols_old, df1_cols_new,
                        df3_cols_old, df3_cols_new)
df13 <- merge_1_3_out$df_merged

# merge df13 with df2
venn_13_2b <- inspect_columns(names(df13), names(df2))
df2_cols_old <- c("Anonymized_Identifier")
df2_cols_new <- c("Unique_Identifier")
merge_13_2_out <- merge_dfs(df13, df2,
                            character(0), character(0),
                            df2_cols_old, df2_cols_new)

## ! NOTE: Consider in df2 there are columns "Arrest_Created_By", "Arrest_Create_By", and "Arrested_Created_By" that may be the same field
## ! but with different spellings. 

df132 <- merge_13_2_out$df_merged

# merge df132 with df4
venn_132_4b <- inspect_columns(names(df132), names(df4))

# Need to merge "Alien_File_Number" with "Alien_File_Numbe" in df4

df4$Alien_File_Number <- ifelse(
  is.na(df4$Alien_File_Number),
  df4$Alien_File_Numbe,
  df4$Alien_File_Number
)

df4$Alien_File_Numbe <- NULL # drop the old column

# double check columns again
venn_132_4b <- inspect_columns(names(df132), names(df4))
df4_cols_old <- c("Area_of_Responsibility", "Arrest_Date", "Arrest_Method") # guessing this is for apprehension AOR, Need confirmation from Amber & David
df4_cols_new <- c("Apprehension_AOR", "Apprehension_Date", "Apprehension_Method")
merge_132_4_out <- merge_dfs(df132, df4,
                            NULL, NULL,
                            df4_cols_old, df4_cols_new)
df1324 <- merge_132_4_out$df_merged


# Merge "Arrest_Created_By", "Arrest_Create_By", and "Arrested_Created_By"
df1324$Arrest_Created_By <- ifelse(
  is.na(df1324$Arrest_Created_By),
  df1324$Arrested_Created_By,
  df1324$Arrest_Created_By
)

# Step 2: fill remaining NAs from Arrest_Create_By
df1324$Arrest_Created_By <- ifelse(
  is.na(df1324$Arrest_Created_By),
  df1324$Arrest_Create_By,
  df1324$Arrest_Created_By
)

# Step 3: drop the alternate columns
df1324$Arrested_Created_By <- NULL
df1324$Arrest_Create_By   <- NULL

df_final <- df1324[, c(setdiff(sort(names(df1324)), "source_file"), "source_file")]

# --- Write out Merged Data ---
date_and_time_cols <- names(df_final)[grepl("_Date", names(df_final)) & grepl("_Time", names(df_final))]
date_cols <- setdiff(names(df_final)[grep("Date", names(df_final))], 
                      c("Birth_Date", date_and_time_cols))

## Convert date_cols to Date
df_final[date_cols] <- lapply(df_final[date_cols], function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, "POSIXt")) return(as.Date(x))
  as.Date(as.character(x))
})

# --- Write out final merged dataset
write.csv(df_final,
          file = "data/ice-processed/arrests-merged.csv",
          row.names = FALSE)
