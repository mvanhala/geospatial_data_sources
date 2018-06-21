
library(sf)
library(dplyr)
library(purrr)
library(purrrlyr)
library(stringr)
library(parallel)

one_cty_dist_water <- function(cty_data) {
  aea_proj <- paste(
    "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96",
    "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
  )
  
  dist <- st_distance(
    st_transform(cty_data[["points"]], aea_proj),
    st_transform(cty_data[["water_shp"]], aea_proj)
  ) %>%
    apply(1, min)
  
  tibble(id = cty_data[["points"]][["id"]], dist_to_water = dist)
}

get_dist_to_water <- function(pts, water_shp_list, cty_shp, cores = 1) {
  locs_cty <- counties[["GEOID"]][
    map_int(st_intersects(st_as_sf(pts, coords = c("lon", "lat"), crs = 4326), cty_shp), ~ .[1])
    ]
  
  pts_by_cty <- pts %>%
    mutate(ctfips = locs_cty) %>%
    filter(!is.na(ctfips)) %>%
    group_by(ctfips) %>%
    by_slice(~ st_as_sf(., coords = c("lon", "lat"), crs = 4326), .to = "points") %>%
    ungroup() %>%
    mutate(water_shp = map(ctfips, ~ water_shp_list[[.]])) %>%
    as.list() %>%
    transpose()
  
  if (cores > 1) {
    cl <- makeCluster(cores)
    clusterEvalQ(cl, { library(sf); library(dplyr) })
    pt_water_list <- clusterApplyLB(cl, pts_by_cty, one_cty_dist_water)
    stopCluster(cl)
  } else {
    pt_water_list <- lapply(pts_by_cty, one_cty_dist_water)
  }
  
  left_join(pts, bind_rows(pt_water_list), by = "id")
}


water_files <- list.files("/mnt/data/water/shapefiles", full.names = TRUE) %>%
  setNames(str_extract(basename(.), "[0-9]{5}"))

cl <- makeCluster(36)
clusterEvalQ(cl, library(sf))
water_shp_list <- parLapplyLB(cl, water_files, read_sf)
stopCluster(cl)

counties <- read_sf("/mnt/data/boundaries/tl_2017_us_county") %>%
  st_transform(4326)

sample_locs <- counties %>%
  filter(STATEFP %in% c("17", "18", "19", "29")) %>%
  st_sample(50000) %>%
  st_coordinates() %>%
  as_tibble() %>%
  mutate(id = sprintf(paste0("%0", nchar(nrow(.)), "d"), seq_len(nrow(.)))) %>%
  select(id, lon = X, lat = Y)

loc_water_dist <- get_dist_to_water(sample_locs, water_shp_list, counties, cores = 36)

