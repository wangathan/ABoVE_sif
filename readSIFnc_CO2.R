#########################
#
#   Read in all the OCO2 SIF NC files
#   Filter them spatially for the ABoVE domain
#   Keep track of space/time info 
#
#   The CO2 lite file version
#

require(ncdf4)
require(data.table)
require(foreach)
require(doParallel)
require(raster)
require(lubridate)

registerDoParallel(detectCores())

sif_files = list.files("../../data/sif/dl_co2",
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

parseTimeVar = function(i, timem){

  timev = timem[,i]

  # as dt
  timedt = data.table(timedat = timev)

  # parse it out
  return(c(y = timedt[1,timedat],
           mo= timedt[2,timedat],
           d = timedt[3,timedat],
           h = timedt[4,timedat],
           mi= timedt[5,timedat],
           s = timedt[6,timedat]))

}


# ABoVE domain
ABoVE = shapefile("../../data/ABoVE/ABoVE_Grid_240m_and_30m/ABoVE_30mgrid_tiles_Final.shp")
ABoVE_ll = spTransform(ABoVE, CRS=CRS(projection("+proj=longlat +datum=WGS84")))
limits = extent(ABoVE_ll)
rm(ABoVE)
rm(ABoVE_ll)

print(paste0("There are ", length(sif_files), " NC files to read!!"))

## Read data
## Get pixels
foreach(i = 1:length(sif_files)) %dopar% { 
  #               .combine = function(x,y)rbindlist(list(x,y),use.names=T)
  
  fout=paste0("../../data/sif/csv_co2/ABoVE_SIF_",i,".csv")
  if(i%%100 == 0)print(i)
  if(!file.exists(fout)){

  
    siff = sif_files[i]
  
    # open NC
    nc = nc_open(siff) 
    # get lats/lons
    lats = ncvar_get(nc, "latitude")
    lons = ncvar_get(nc, "longitude")
  
    # get indices based on both lats and lons
    above_lats = lats > limits@ymin & lats < limits@ymax
    above_lons = lons > limits@xmin & lons < limits@xmax
  
    # get indices based on measurement mode (want 0 for nadir)
    measure = ncvar_get(nc, "Sounding/operation_mode")
    measure_nadir = measure == 0

    # only land
    land = ncvar_get(nc, "Sounding/land_water_indicator")
    island = land == 0
  
    inABoVE = above_lats & above_lons & measure_nadir & island
  
    if(!any(inABoVE))return(NULL)
  
    # shape info 
    inLatvs = ncvar_get(nc, "vertex_latitude")[,inABoVE]
    inLonvs = ncvar_get(nc, "vertex_longitude")[,inABoVE]

    # warnlevel
    wl = ncvar_get(nc, "warn_level")[inABoVE]
    #inLatvs = latvs[,inABoE] 
    #inLonvs = lonvs[,inABoVE] 
  
    # data
    SIF = ncvar_get(nc, "Retrieval/fs")[inABoVE]
    aod_dust = ncvar_get(nc, "Retrieval/aod_dust")[inABoVE]
    aod_water = ncvar_get(nc, "Retrieval/aod_water")[inABoVE]
    aod_ice = ncvar_get(nc, "Retrieval/aod_ice")[inABoVE]
    aod_sulfate = ncvar_get(nc, "Retrieval/aod_sulfate")[inABoVE]
    aod_total = ncvar_get(nc, "Retrieval/aod_total")[inABoVE]
    aod_bc = ncvar_get(nc, "Retrieval/aod_bc")[inABoVE]
    aod_oc = ncvar_get(nc, "Retrieval/aod_oc")[inABoVE]
    aod_seasalt = ncvar_get(nc, "Retrieval/aod_seasalt")[inABoVE]
 
    data_matr = cbind(SIF, aod_dust, aod_ice, aod_sulfate, aod_bc, aod_oc, aod_seasalt, aod_total)

    # process time data
    nctime = ncvar_get(nc, "date")[,inABoVE]

    if(is.null(ncol(nctime)) & !is.null(length(nctime))){
    
      nctime = matrix(nctime, ncol=1, byrow=T)
    
    }
    time_matr_l=lapply(1:ncol(nctime),parseTimeVar, timem = nctime)
    time_matr = do.call(rbind, time_matr_l)
  
    nc_close(nc)

    # footprint data
    if(is.null(ncol(inLatvs)) & !is.null(length(inLatvs))){
    
      inLatvs = matrix(inLatvs, ncol=1, byrow=T)
      inLonvs = matrix(inLonvs, ncol=1, byrow=T)
    
    }
    ll_matr_l=lapply(1:ncol(inLatvs),convertVertex, latvs=inLatvs, lonvs=inLonvs)
    ll_matr = do.call(rbind, ll_matr_l)
  
    # combine
    nc_matr = cbind(time_matr, ll_matr, data_matr)
    nc_dt = as.data.table(nc_matr)
#    numercols = c("SIF771", "SIF757", "LL_lon", "LL_lat", "UL_lon", "UL_lat", "UR_lon", "UR_lat", "LR_lon", "LR_lat","temp2m", "vpd")
#    nc_dt[, lapply(.SD, as.numeric), .SDcols = numercols]
#    setnames(nc_dt, "V1", "nctime")
  
    write.csv(nc_dt, fout, row.names=F)
  } 

}

