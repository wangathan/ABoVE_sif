#####
#
#   Oops the csv wrote as all character??? Bring it back to numeric
#   Go ahead and just run this interactively

require(data.table)
require(lubridate)

sifdt = fread("../../data/sif/csv/ABoVE_SIF_all.csv")

numercols = c("SIF771", "SIF757", "LL_lon", "LL_lat", "UL_lon", "UL_lat", "UR_lon", "UR_lat", "LR_lon", "LR_lat","temp2m", "vpd")
timecol = sifdt$nctime
sifdt = sifdt[, lapply(.SD, as.numeric), .SDcols = numercols]
sifdt[, nctime := ymd_hms(timecol)]
setcolorder(sifdt, c("nctime", numercols))
write.csv(sifdt,"../../data/sif/csv/ABoVE_SIF_all.csv", row.names=F) 
