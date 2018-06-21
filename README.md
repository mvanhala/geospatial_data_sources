# Geospatial data sources

This repo contains code for downloading and extracting data from several different geospatial data sources, and for using this data to enrich other spatial data sets.

The following are the spatial data sources:

* Elevation data from the [National Elevation Dataset](https://nationalmap.gov/elevation.html) from the [USGS's National Map](https://nationalmap.gov/)

* [Bodies of water](https://www2.census.gov/geo/tiger/TIGER2017/AREAWATER/) in the United States from the [TIGER products](https://www.census.gov/geo/maps-data/data/tiger.html) published by the US Census Bureau

* Soil map unit and attribute from the [SSURGO database](https://datagateway.nrcs.usda.gov/) produced by the National Resources Conservation Service and published on the [NRCS Geospatial Data Gateway](https://datagateway.nrcs.usda.gov/)

These include both raster (elevation) and vector (water, soil) spatial data types. In the examples provided, the data sets to be enriched consists of spatial point data, but the building blocks in this code could readily be adapted to line or polygon data, depending on the desired approach.

Spatial operations are all performed using the relatively new [sf R package](https://cran.r-project.org/web/packages/sf/index.html), which has become the standard for spatial data operations in R, taking the role previously held by the sp/rgdal/rgeos collection of packages.

