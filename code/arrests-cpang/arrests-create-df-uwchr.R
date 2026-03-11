# --- Clear Memory --- 
rm(list=ls())

# --- Packages --- 
library(readxl)
library(dplyr)
library(purrr)
library(janitor)
library(tibble)
library(stringr)
library(arrow)

# --- Source Functions ---
source("code/functions/process_folder_data_v2.R")

folder_dir = "data/ice-raw/arrests-selected/uwchr"
pattern = "\\.xlsx$"
recursive = TRUE
anchor_idx = 2
guess_max = 10000


