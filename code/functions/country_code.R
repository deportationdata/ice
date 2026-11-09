library(countrycode)

# GENC 3-letter code (US government ISO 3166 profile; the geoid of
# us-shapefiles' countries-sf.parquet) from ICE's country names. Values that
# have no code are listed so that only new names warn.
country_code <- function(x) {
  countrycode(
    x,
    "country.name",
    "genc3c",
    custom_match = c(
      "KOREA" = "KOR", # ambiguous; SOUTH KOREA and NORTH KOREA are coded separately
      "CONGO" = "COG",
      "DEM REP OF THE CONGO" = "COD",
      "SINT MAARTEN(DUTCH)" = "SXM",
      "SAINT MARTIN(FRENCH)" = "MAF",
      "SINT EUSTATIUS" = "BES",
      "SABA" = "BES",
      "BONAIRE" = "BES",
      "PALESTINE BORN BEFORE 1948" = NA, # GENC codes the West Bank and Gaza, not Palestine
      "USSR" = NA,
      "YUGOSLAVIA" = NA,
      "CZECHOSLOVAKIA" = NA,
      "SERBIA AND MONTENEGRO" = NA,
      "NETHERLANDS ANTILLES" = NA,
      "EUROPEAN UNION" = NA,
      "ARABIAN PENINSULA" = NA,
      "OCEANIA" = NA,
      "NON-QUOTA IMMIGRANT" = NA,
      "WESTERN SAHARA" = NA, # no GENC code: shown as part of Morocco
      "UNKNOWN" = NA,
      "STATELESS" = NA
    )
  )
}
