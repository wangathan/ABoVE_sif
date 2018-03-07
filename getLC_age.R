################
#
#   Get ABoVE_SIF csv
#   For each row, 
#     - look up ABoVE tile
#     - build tiny shapefile
#     - reproject to ABoVE and extract: LC and Age from EOSD and C2C
#     - Age as average, LC as ... proportions of certain ones? DBF, ENF, GRA ... or pick primary, secondary, and tertiary classes
#     - Save
#     - It becomes a sweet new paper
#
#

require(data.table)
require(raster)
require(rgdal)
require(foreach)
require(doParallel)
require(rgeos)
require(parallel)

sifdt = fread("../../data/sif/csv_co2/ABoVE_SIF_all.csv")

registerDoParallel(14)

block = commandArgs(TRUE)[1]
block = as.numeric(block)

getPoly = function(i, sifdt){

  # extract row  coordinates

  theRow = sifdt[i,]
  llmatr = matrix(as.numeric(theRow[1,c("LL_lon", "LL_lat",
                                         "UL_lon", "UL_lat",
                                         "UR_lon", "UR_lat",
                                         "LR_lon", "LR_lat"),
                                         with=F]),
                  byrow=T,
                  ncol=2)

  # turn into spatial object

  thePoly = SpatialPolygons(list(Polygons(list(Polygon(llmatr)), 1)))
  crs(thePoly) = CRS(projection("+proj=longlat +datum=WGS84"))

  return(thePoly)

}

agrid = shapefile("../../data/ABoVE/ABoVE_Grid_240m_and_30m/ABoVE_30mgrid_tiles_Final.shp")
abdt = as.data.table(agrid@data)

aboveCheck = function(smp,a) {
    tile = agrid[a,]
  if(gIntersects(smp, tile))
      {
            return(as.numeric(as.character(tile$UID)))
    }else{
          return(-999)
      }
}

# to determine which dataset to get
getTile = function(i, sifdt){

  # reproject to ABoVE 
  #  thePoly = getPoly(i, sifdt)
  meanLon = mean(unlist(sifdt[i, .(LL_lon, UL_lon, LR_lon, UR_lon)]))
  meanLat = mean(unlist(sifdt[i, .(LL_lat, UL_lat, LR_lat, UR_lat)]))

  if(is.na(meanLon) | is.na(meanLat))return(NA)

  ll_matr = matrix(c(meanLon, meanLat),
                  ncol=2, byrow=T) 

  ll_df = as.data.frame(ll_matr)
  coordinates(ll_df) = c("V1", "V2")
  proj4string(ll_df) = CRS("+proj=longlat +datum=WGS84")

  aea = spTransform(ll_df, CRS(projection(agrid)))

  # intersect
  tilepick = unlist(lapply(1:nrow(agrid), aboveCheck, smp = aea))
  theTile = agrid[which(tilepick!=-999),]

  # convert nomenclature
  abovedt = as.data.table(theTile@data)
  abovedt[, names(abovedt):=lapply(.SD, as.character)]
  abovedt[, names(abovedt):=lapply(.SD, as.numeric)]
  abovedt[, c("theBh", "theBv"):=.(Ahh * 6 + Bh, Avv*6 + Bv)]
  abovedt[,tileid := paste0("Bh",sprintf("%02d",theBh),"v",sprintf("%02d",theBv))]

  # return
  if(nrow(theTile) == 0)return(NA)
  if(nrow(theTile) > 1)return(NA)
  return(abovedt[1,tileid])
}

# returning errors, not sure why
#tilelist = unlist(mclapply(1:nrow(sifdt),getTile, siftdt=sifdt, mc.cores=detectCores()))
#tilelist = unlist(lapply(1:nrow(sifdt), getTile, sifdt=sifdt))

#tilelist = foreach(i = 1:nrow(sifdt),
#tilelist <- foreach(i = 1:1000,
#                   .combine = c) %dopar% {

#  if(i %% 1000 == 0)print(i)

#  return(getTile(i, sifdt=sifdt))
  

#}

#print("Tilelist generated")

#sifdt[, theTile := tilelist]
#sifdt = na.omit(sifdt)

#write.csv(sifdt, "../../data/sif/csv_co2/ABoVE_SIF_all_tiles.csv", row.names=F)

C2C_tiles  = list.files("/projectnb/landsat/users/dsm/above/cfs_change/C2C_change_year",
                        full.names=T)
EOSD_tiles = list.files("/projectnb/landsat/users/dsm/above/cfs_change/EOSD",
                        full.names=T)

# for each row
# look up tile
# get top three land covers and their proportions
# get proportion disturbed and mean age of disturbed pixels

getLandData = function(i, sifdt){

  #ti = sifdt[i, theTile]
  ti = getTile(i, sifdt)
  if(is.na(ti))return(NULL)
  # check if tile is in domain
  if(!any(grepl(ti, EOSD_tiles)))return(NULL)
  if(!any(grepl(ti, C2C_tiles)))return(NULL)
  EOSD = raster(EOSD_tiles[grepl(ti, EOSD_tiles)])
  C2C = raster(C2C_tiles[grepl(ti, C2C_tiles)])

  fp = getPoly(i, sifdt)
  fp_aea = spTransform(fp, CRS(projection(EOSD)))

  EOSDex = unlist(suppressWarnings(extract(EOSD, fp_aea)))
  C2Cex = unlist(suppressWarnings(extract(C2C, fp_aea)))

  # disturbance data
  propDist = sum(C2Cex[C2Cex!=0])/length(C2Cex)
  distYears = 1900 + C2Cex[C2Cex!=0]
  meanAge = mean(sifdt[i, y] - distYears, na.rm=T)

  # land cover data
  if(is.null(EOSDex))return(NULL)
  lcdt = data.table(lc = EOSDex)

  # drop water clouds and other junk
  lcdt = lcdt[!lc %in% c(0, 11, 12, 20, 31, 32, 33),] 

  lcdt[,count := .N,by=lc]
  lcdt = unique(lcdt)
  setorder(lcdt, -count)
  scount = sum(lcdt$count)
  lcdt[,freq:=count/scount]

  lc1 = ifelse(nrow(lcdt)>0,lcdt[1, lc],NA)
  lc2 = ifelse(nrow(lcdt)>1,lcdt[2, lc],NA)
  lc3 = ifelse(nrow(lcdt)>2,lcdt[3, lc],NA)
  lc4 = ifelse(nrow(lcdt)>3,lcdt[4, lc],NA)
  
  freq1 = ifelse(nrow(lcdt)>0,lcdt[1, freq],NA)
  freq2 = ifelse(nrow(lcdt)>1,lcdt[2, freq],NA)
  freq3 = ifelse(nrow(lcdt)>2,lcdt[3, freq],NA)
  freq4 = ifelse(nrow(lcdt)>3,lcdt[4, freq],NA)

  # produce a data table row!

  theRow = sifdt[i,]
  theRow[, c("tile", "propDist", "meanAge", "lc1", "freq1", "lc2", "freq2", "lc3", "freq3", "lc4", "freq4") :=
           .(ti, propDist, meanAge, lc1, freq1, lc2, freq2, lc3, freq3, lc4, freq4)]

}

print('reading land cover data')


blockstart =(block - 1)*500000 + 1
blockend = blockstart + 500000
if(blockend > nrow(sifdt))blockend=nrow(sifdt)

#siflcdt = foreach(i = 1:nrow(sifdt),
siflcdt = foreach(i = blockstart:blockend,
#system.time(siflcdt <- foreach(i = 1:2000,
                  .combine = function(x,y)rbindlist(list(x,y),use.names=T)) %dopar% {

  if(i %% 1000 == 0)print(i)
  return(getLandData(i, sifdt))


}

write.csv(siflcdt, paste0("../../data/sif/csv_co2/ABoVE_SIF_all_lc_block_",block,".csv"), row.names=F)



