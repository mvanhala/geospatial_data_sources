
# before extracting, download state-level zip files with soil maps within the state
# from NRCS Geospatial Data Gateway at https://datagateway.nrcs.usda.gov/
# files saved as {state postal abbreviation}.zip in /mnt/data/ssurgo/zip

library(stringr)
library(purrr)
library(parallel)

extract_one_soil_file <- function(soil_pars, state, root_dir, tmp) {
  soil_file <- soil_pars$file
  region_code <- soil_pars$code
  
  unzip(soil_file, exdir = tmp)
  
  tmp_soil_shp <- list.files(
    file.path(tmp, region_code, "spatial"),
    pattern = "soilmu_a_", 
    full.names = TRUE
  )
  dest_soil_shp <- file.path(root_dir, "shapefiles", state, basename(tmp_soil_shp))
  file.copy(tmp_soil_shp, dest_soil_shp)
  
  file.copy(
    file.path(tmp, region_code, "tabular", "muaggatt.txt"),
    file.path(root_dir, "muaggatt", state, paste0(region_code, "_muaggatt.txt"))
  )
  file.copy(
    file.path(tmp, region_code, "readme.txt"),
    file.path(root_dir, "readme", state, paste0(region_code, "_readme.txt"))
  )
}

extract_soil_state <- function(state, root_dir = ".", cores = 1) {
  tmp <- tempfile(tmpdir = root_dir)
  on.exit(unlink(tmp, recursive = TRUE))
  unzip(file.path(root_dir, "zip", paste0(state, ".zip")), exdir = tmp)
  
  soil_files <- list.files(tmp, pattern = ".zip", full.names = TRUE)
  region_codes <- str_extract(soil_files, "[A-Z]{2}[0-9]{3}")
  soil_pars <- list(file = soil_files, code = region_codes) %>%
    transpose
  
  dir.create(file.path(root_dir, "shapefiles", state))
  dir.create(file.path(root_dir, "muaggatt", state))
  dir.create(file.path(root_dir, "readme", state))
  
  cl <- makeCluster(cores)
  parLapply(cl, soil_pars, extract_one_soil_file, state = state, root_dir = root_dir, tmp = tmp)
  stopCluster(cl)
  
  invisible(TRUE)
}

dir.create("/mnt/data/ssurgo/shapefiles")
dir.create("/mnt/data/ssurgo/muaggatt")
dir.create("/mnt/data/ssurgo/readme")

walk(c(state.abb, "DC"), extract_soil_state, root_dir = "/mnt/data/ssurgo", cores = 8)



