
library(raster)
library(sf)
library(dplyr)
library(purrr)
library(purrrlyr)
library(tools)
library(stringr)
library(parallel)

one_box_elevation <- function(box_data) {
  if (!is.null(box_data[["elev_raster"]])) {
    elev <- extract(box_data[["elev_raster"]], box_data[["points"]], method = "bilinear")
  } else {
    elev <- NA_real_
  }
  tibble(id = box_data[["points"]][["id"]], elevation = elev)
}

get_elevation <- function(pts, elev_rasters, cores = 1) {
  pts_by_box <- pts %>%
    mutate(
      n_value = ceiling(lat),
      w_value = str_pad(abs(floor(lon)), width = 3, side = "left", pad = "0"),
      box = paste0("n", n_value, "w", w_value)
    ) %>%
    group_by(box) %>%
    by_slice(~ st_as_sf(., coords = c("lon", "lat")), .to = "points") %>%
    mutate(elev_raster = map(box, ~ elev_rasters[[.]])) %>%
    as.list() %>%
    transpose()
  
  if (cores > 1) {
    cl <- makeCluster(cores)
    clusterEvalQ(cl, { library(raster); library(dplyr) })
    pt_elev_list <- clusterApplyLB(cl, pts_by_box, one_box_elevation)
    stopCluster(cl)
  } else {
    pt_elev_list <- lapply(pts_by_box, one_box_elevation)
  }
  
  all_pt_elev <- bind_rows(pt_elev_list)
  left_join(pts, all_pt_elev, by = "id")
}


box_files <- list.files("/mnt/data/elevation/img", full.names = TRUE)
boxes <- setNames(box_files, file_path_sans_ext(basename(box_files)))
elev_rasters <- map(boxes, raster) 

states <- read_sf("/mnt/data/boundaries/tl_2017_us_state") %>%
  st_transform(4326)

sample_locs <- states %>%
  filter(STUSPS %in% c("TX", "OK", "KS", "CO", "NM")) %>%
  st_sample(50000) %>%
  st_coordinates() %>%
  as_tibble() %>%
  mutate(id = sprintf(paste0("%0", nchar(nrow(.)), "d"), seq_len(nrow(.)))) %>%
  select(id, lon = X, lat = Y)

loc_elevations <- get_elevation(sample_locs, elev_rasters, cores = 36)

