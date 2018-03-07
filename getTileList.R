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

sifdt = fread("../../data/sif/csv/ABoVE_SIF_all.csv")

registerDoParallel(detectCores())

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
  # if(nrow(theTile) == 1)return(NA)
  # if(nrow(theTile) > 1)return(NA)
  return(abovedt[1,tileid])
}

getTileData= function(i, sifdt){

  #ti = sifdt[i, theTile]
  ti = getTile(i, sifdt)
  if(is.na(ti))return(NULL)
  
  # produce a data table row!

  theRow = sifdt[i,]
  theRow[, tile:=ti]
  return(theRow)

}


blockstart =(block - 1)*150000 + 1
blockend = blockstart + 150000
if(blockend > nrow(sifdt))blockend=nrow(sifdt)

#siflcdt = foreach(i = 1:nrow(sifdt),
siftiles <- foreach(i = blockstart:blockend,
                  .combine = function(x,y)rbindlist(list(x,y),use.names=T)) %dopar% {

  if(i %% 1000 == 0)print(i)
  return(getTileData(i, sifdt))


}

write.csv(siftiles, paste0("../../data/sif/csv/ABoVE_SIF_all_tile_block_",block,".csv"), row.names=F)



