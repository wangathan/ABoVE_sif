######
#
#   A little script to learn a bit about SIF products
#
#
#

require(raster)
require(ncdf4)
require(data.table)
require(foreach)
require(doParallel)
require(rgeos)
require(rgdal)
require(ggplot2)

sif_files = list.files("../../data/sif/dl",
                       full.names=T)

# each is global?
# how are they gridded?
# what is res?

test = nc_open(sif_files[1])

# variables
attributes(test$var)$names

# key spacetime
# latitude, longitude, time, sounding_id, orbit_number, footprint, footprint_vertex_latitude, footprint_vertex_longitude
# sounding_id: unique id for each sounding as YYYYMMDDHHMMSS
# lat/lon are center
# time is in seconds since 1993-1-1 0:0:0
# footprint - there are 8 independent spatial sampels per frame
# footprint_vertex_latitude and footprint_vertex_longitude - 4-row matrix indicating each of the corners of each footprint

# measurement meta
# measurement_mode, surface_altitude, solar_zenith_angle, solar_azimuth_angle, sensor_azimuth_angle, sensor_zenith_angle
# measurement_mode: 0=Nadir, 1=Glint, 2=Target

# data
# SIF_757nm, SIF_771nm

# aux data
# Meteo/surface_pressure, 
# Meteo/specific_humidity, 
# Meteo/vapor_pressure_deficit, 
# Meteo/skin_temperature, 
# Meteo/2m_temperature, 
# Meteo/wind_speed, 
# Meteo/low_cloud_cover, 
# Meteo/mid_cloud_cover, 
# Meteo/high_cloud_cover
# Cloud/cloud_flag, Cloud/albedo

# cloud_flag: 0 is clear, 1 is processing failed, 2 not classified


# 
# do we need orbit tracks to map them to pixels?

ncatt_get(test, "latitude") #to learn about each variable

# matrix of data
fs = ncvar_get(test, "SIF_757nm")
dim(fs) # 104006
lat = ncvar_get(test, "vertex_latitude")
dim(lat) # 104006
lon = ncvar_get(test, "longitude")
dim(lon) # 104006

# global attributes
nc_atts = ncatt_get(test,0)
