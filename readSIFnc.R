#########################
#
#   Read in all the OCO2 SIF NC files
#   Filter them spatially for the ABoVE domain
#   Keep track of space/time info 
#
#

require(ncdf4)
require(data.table)
require(foreach)
require(doParallel)
require(raster)
require(lubridate)

registerDoParallel(detectCores())

sif_files = list.files("../../data/sif/dl",
                       full.names=T,
                       pattern="nc4$")

convertVertex = function(i, latvs, lonvs){

  lonv = lonvs[,i]
  latv = latvs[,i]

  # put into datatable
  lldt = data.table(lat = latv, lon = lonv)
  
  # actually we're not going to assume anything about the rectitude of these rectangles
  # just going it manually
  return(c(LL_lat = lldt[1,lat], LL_lon = lldt[1,lon], 
           UL_lat = lldt[2,lat], UL_lon = lldt[2,lon],
           UR_lat = lldt[3,lat], UR_lon = lldt[3,lon], 
           LR_lat = lldt[4,lat], LR_lon = lldt[4,lon])) 

}


# ABoVE domain
ABoVE = shapefile("../../data/ABoVE/ABoVE_Grid_240m_and_30m/ABoVE_30mgrid_tiles_Final.shp")
ABoVE_ll = spTransform(ABoVE, CRS=CRS(projection("+proj=longlat +datum=WGS84")))
limits = extent(ABoVE_ll)
rm(ABoVE)
rm(ABoVE_ll)

## Read data
## Get pixels
nc_dt  = foreach(i = 1:length(sif_files),
                 .combine = function(x,y)rbindlist(list(x,y),use.names=T)) %dopar% {
  
  siff = sif_files[i]

  # open NC
  nc = nc_open(siff) 
  # get lats/lons
  latvs = ncvar_get(nc, "footprint_vertex_latitude")
  lonvs = ncvar_get(nc, "footprint_vertex_longitude")
  lats = ncvar_get(nc, "latitude")
  lons = ncvar_get(nc, "longitude")

  # get indices based on both lats and lons
  above_lats = lats > limits@ymin & lats < limits@ymax
  above_lons = lons > limits@xmin & lons < limits@xmax

  # get indices based on measurement mode (want 0 for nadir)
  measure = ncvar_get(nc, "measurement_mode")
  measure_nadir = measure == 0

  # get indices based on Cloud/cloud_flag (want 0 for clear)
  clouds = ncvar_get(nc, "Cloud/cloud_flag")
  clouds_clear = clouds==0

  inABoVE = above_lats & above_lons & clouds_clear & measure_nadir
 
  # shape info 
  inLatvs = latvs[,inABoVE] 
  inLonvs = lonvs[,inABoVE] 

  # data
  SIF757 = ncvar_get(nc, "SIF_757nm")[inABoVE]
  SIF771 = ncvar_get(nc, "SIF_771nm")[inABoVE]

  # process time data
  nctime = ncvar_get(nc, "time")[inABoVE]
  time_str = ymd_hms("1993-1-1 0:0:0") + seconds_to_period(nctime)

  # met data
  vpd = ncvar_get(nc, "Meteo/vapor_pressure_deficit")[inABoVE]
  temp2m = ncvar_get(nc, "Meteo/2m_temperature")[inABoVE]
  #lowCloud = ncvar_get(nc, "Meteo/low_cloud_cover")[inABoVE]
  #midCloud = ncvar_get(nc, "Meteo/mid_cloud_cover")[inABoVE]
  #highCloud = ncvar_get(nc, "Meteo/high_cloud_cover")[inABoVE]
  
  # footprint data
  ll_matr_l=lapply(1:ncol(inLatvs),convertVertex, latvs=inLatvs, lonvs=inLonvs)
  ll_matr = do.call(rbind, ll_matr_l)

  # combine
  nc_matr = cbind(as.character(time_str), ll_matr, SIF757, SIF771, vpd, temp2m)
  nc_dt = as.data.table(nc_matr)
  numercols = c("SIF771", "SIF757", "LL_lon", "LL_lat", "UL_lon", "UL_lat", "UR_lon", "UR_lat", "LR_lon", "LR_lat","temp2m", "vpd")
  nc_dt[, lapply(.SD, as.numeric), .SDcols = numercols]
  setnames(nc_dt, "V1", "nctime")

}


