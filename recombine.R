########
#
#   A quick script to recombine the NC-processing outputs
#

require(data.table)
require(foreach)
require(doParallel)

registerDoParallel(detectCores())

files = list.files("../../data/sif/csv_co2",
                   pattern="ABoVE_SIF_[0-9]*.csv",
                   full.names=T)

bigcsv = foreach(i = 1:length(files),
                 .combine=function(x,y)rbindlist(list(x,y),use.names=T)) %dopar% {
  print(i)
  return(fread(files[i]))

}

write.csv(bigcsv, "../../data/sif/csv_co2/ABoVE_SIF_all.csv", row.names=F)
