
library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(readr)
library(parallel)

states <- read_sf("/mnt/data/boundaries/tl_2017_us_state") %>%
  st_transform(4326)

system(
  glue::glue(
    "aws configure set aws_access_key_id {id}", 
    id = Sys.getenv("AWS_ACCESS_KEY_ID")
  )
)

system(
  glue::glue(
    "aws configure set aws_secret_access_key {key}", 
    key = Sys.getenv("AWS_SECRET_ACCESS_KEY")
  )
)

ned_files <- system("aws s3 ls s3://prd-tnm/StagedProducts/Elevation/1/IMG/", intern = TRUE)

zip_files <- ned_files %>%
  keep(str_detect, pattern = ".zip$")

times <- str_sub(zip_files, end = 19)

img_files <- zip_files %>%
  str_sub(start = 20) %>%
  str_trim() %>%
  str_split("[ ]+", n = 2) %>%
  transpose() %>%
  simplify_all() %>%
  setNames(c("size", "name")) %>%
  as_tibble() %>%
  mutate(updated_time = times)

boxes <- img_files %>%
  mutate(
    area = str_extract(name, "n[0-9]+w[0-9]+"),
    lat = parse_number(str_extract(area, "n[0-9]+")),
    lon = parse_number(str_extract(area, "w[0-9]+")),
    url = paste0("https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/1/IMG/", name)
  ) %>%
  filter(!is.na(area)) %>%
  group_by(area) %>%
  filter(row_number(desc(updated_time)) == 1) %>%
  ungroup() 

boxes_poly <- st_as_sf(
  boxes, 
  geom = st_sfc(
    map2(
      boxes$lon, boxes$lat, 
      function(x, y) {
        st_polygon(list(cbind(c(-x, -x + 1, -x + 1, -x, -x), c(y, y, y - 1, y - 1, y))))
      }
    ),
    crs = 4326
  )
)

boxes_us <- boxes_poly %>%
  mutate(in_us = as.logical(st_intersects(., st_union(states), sparse = FALSE)))

boxes_download <- boxes_us %>%
  filter(in_us) %>%
  as.list() %>%
  .[c("area", "url")] %>%
  transpose()

download_one_box <- function(pars, root_dir = ".") {
  url <- pars$url
  area <- pars$area
  zip_path <- file.path(root_dir, "zip", paste0(area, ".zip"))
  download.file(url, zip_path, quiet = TRUE)
  tmp <- tempfile(tmpdir = root_dir)
  on.exit(unlink(tmp, recursive = TRUE))
  unzip(zip_path, exdir = tmp)
  img_files <- list.files(tmp, pattern = ".img|.IMG", full.names = TRUE)
  if (length(img_files) > 0) {
    first_img <- img_files[[1]]
    file.copy(
      file.path(first_img), 
      file.path(root_dir, "img", paste0(area, ".img"))
    )
  }
}

dir.create("/mnt/data/elevation")
dir.create("/mnt/data/elevation/zip")
dir.create("/mnt/data/elevation/img")

cl <- makeCluster(36)
ned_downloads <- parLapply(
  cl, boxes_download, download_one_box, root_dir = "/mnt/data/elevation"
)
stopCluster(cl)


