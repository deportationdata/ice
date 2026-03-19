rm(list=ls())

print("Building Dataframes from folders...")
source("code/01_detentions_create_df.R")
print("Done.")

# Code below is optional
print("Determine which data sources to use...")
source("code/02_inspect_detentions_sources.R")

print("Building Master dataframe for Detentions... ")
source("code/03_build_detentions_master")
print("Done.")

print("Flag Duplicates for Detentions...")
source("code/04_flag_detention_duplicates.R")
print("Done.")