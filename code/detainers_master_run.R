rm(list=ls())

print("Building Dataframes from folders...")
source("code/01_detainers_create_df.R")
print("Finished data frame creation.")

print("Create Master dataframe...")
source("code/02_build_detainers_master.R")
print("Finished master dataframe.")
