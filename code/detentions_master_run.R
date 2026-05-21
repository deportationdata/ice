rm(list=ls())

print("Building Dataframes from folders...")
source("code/detentions-prep.R")
print("Done.")

# Code below is optional
print("Determine which data sources to use...")
source("code/inspect-detentions-sources.R")

print("Building Master dataframe for Detentions... ")
source("code/detentions-historical.R")
print("Done.")

print("Flag Duplicates for Detentions...")
source("code/detentions-flag-duplicates.R")
print("Done.")