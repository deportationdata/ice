rm(list=ls())

library(dplyr)
library(arrow)

# read data
arrests_counts <- read_feather("data/ice-counts/arrests-weekly-counts.feather")
detainers_counts <- read.feather("data")