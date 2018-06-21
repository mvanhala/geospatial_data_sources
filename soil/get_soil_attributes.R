
library(sf)
library(dplyr)
library(purrr)
library(purrrlyr)
library(parallel)
library(tidyr)
library(readr)

get_one_soil_map <- function(soil_file, state_pt_list) {
  path <- soil_file[["path"]]
  state <- soil_file[["state"]]
  soil_shp <- read_sf(path)
  mukeys <- soil_shp[["MUKEY"]][
    map_int(st_intersects(state_pt_list[[state]], soil_shp), ~ .[1])
    ]
  tibble(id = state_pt_list[[state]][["id"]], mukey = mukeys) %>%
    filter(!is.na(mukey))
}

get_soil_mukey <- function(pts, states, root_dir = ".", cores = 1) {
  state_pts <- states[["STUSPS"]][
    map_int(st_intersects(st_as_sf(pts, coords = c("lon", "lat"), crs = 4326), states), ~ .[1])
    ]
  
  pts_by_state <- pts %>%
    mutate(state_abb = state_pts) %>%
    group_by(state_abb) %>%
    by_slice(~ st_as_sf(., coords = c("lon", "lat"), crs = 4326), .to = "points") %>%
    ungroup() 
  
  state_pt_list <- setNames(pts_by_state[["points"]], pts_by_state[["state_abb"]])
  
  soil_files <- tibble(state = names(state_pt_list)) %>%
    mutate(
      folder = file.path(root_dir, "shapefiles", state),
      path = map(folder, list.files, ".shp", full.names = TRUE)
    ) %>%
    unnest(path) %>%
    select(state, path) %>%
    as.list() %>%
    transpose()
  
  if (cores > 1) {
    cl <- makeCluster(cores)
    clusterEvalQ(cl, { library(sf); library(dplyr); library(purrr) })
    mukey_list <- clusterApplyLB(
      cl, soil_files, get_one_soil_map, state_pt_list = state_pt_list
    )
    stopCluster(cl)
  } else {
    mukey_list <- lapply(soil_files, get_one_soil_map, state_pt_list = state_pt_list)
  }
  
  left_join(pts, bind_rows(mukey_list), by = "id")
}


add_soil_attributes <- function(pts, root_dir = ".", cores = 1) {
  mapunit_attr_files <- list.files(
    file.path(root_dir, "muaggatt"), 
    full.names = TRUE, 
    recursive = TRUE
  )
  
  cl <- makeCluster(cores)
  clusterEvalQ(cl, library(readr))
  mapunit_attr <- parLapply(
    cl,
    mapunit_attr_files, 
    read_delim, 
    delim = "|",
    col_names = c(
      "musym", "muname", "mustatus", "slopegraddcp", "slopegradwta", 
      "brockdepmin", "wtdepannmin", "wtdepaprjunmin", "flodfreqdcd", "flodfreqmax", 
      "pondfreqprs", "aws025wta", "aws050wta", "aws0100wta", "aws0150wta", 
      "drclassdcd", "drclasswettest", "hydgrpdcd", "iccdcd", "iccdcdpct", 
      "niccdcd", "niccdcdpct", "engdwobdcd", "engdwbdcd", "engdwbll", 
      "engdwbml", "engstafdcd", "engstafll", "engstafml", "engsldcd",
      "engsldcp", "englrsdcd", "engcmssdcd", "engcmssmp", "urbecptdcd", 
      "urbecptwta", "forpehrtdcp", "hydclprs", "awmmfpwwta", "mukey"
    ),
    col_types = "cccddiiicccddddccccicicccccccccccccdccdc"
  ) %>%
    bind_rows()
  stopCluster(cl)
  
  left_join(
    pts,
    mapunit_attr %>%
      group_by(mukey) %>%
      filter(row_number() == 1) %>%
      ungroup() %>%
      select(mukey, hydgrpdcd),
    by = "mukey"
  )
}


states <- read_sf("/mnt/data/boundaries/tl_2017_us_state") %>%
  st_transform(4326)

sample_locs <- states %>%
  filter(STUSPS %in% c("IA", "MO", "KS", "NE", "SD")) %>%
  st_sample(100000) %>%
  st_coordinates() %>%
  as_tibble() %>%
  mutate(id = sprintf(paste0("%0", nchar(nrow(.)), "d"), seq_len(nrow(.)))) %>%
  select(id, lon = X, lat = Y)

locs_with_soil_mukey <- get_soil_mukey(sample_locs, states, "/mnt/data/ssurgo", 36)

locs_with_soil_attr <- add_soil_attributes(locs_with_soil_mukey, "/mnt/data/ssurgo", 8)


