
library(xml2)
library(purrr)
library(dplyr)
library(stringr)
library(tools)
library(parallel)

base_url <- "https://www2.census.gov/geo/tiger/TIGER2017/AREAWATER/"
water_page <- read_html(base_url)

county_links <- water_page %>% 
  xml_find_all(".//a") %>% 
  xml_attr("href") %>%
  keep(str_detect, pattern = "tl_2017_[0-9]{5}_areawater.zip")

water_urls <- tibble(filename = county_links) %>%
  mutate(
    url = paste0(base_url, filename),
    fips = str_extract(url, "[0-9]{5}"),
    state_fips = str_sub(fips, start = 1, end = 2),
    county_fips = str_sub(fips, start = 3, end = 5)
  ) %>%
  filter(as.numeric(state_fips) <= 56)

download_water_shp <- function(url, root_dir = ".") {
  zip_path <- file.path(root_dir, "zip", basename(url))
  shp_dir <- file_path_sans_ext(basename(url))
  download.file(url, zip_path, quiet = TRUE)
  unzip(zip_path, exdir = file.path(root_dir, "shapefiles", shp_dir))
  invisible(0)
}

dir.create("/mnt/data/water")
dir.create("/mnt/data/water/zip")
dir.create("/mnt/data/water/shapefiles")

cl <- makeCluster(36)
clusterEvalQ(cl, library(tools))
water_files <- parLapply(
  cl, water_urls[["url"]], download_water_shp, root_dir = "/mnt/data/water"
)
stopCluster(cl)


