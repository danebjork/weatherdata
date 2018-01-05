# NOAA Weather Collection

This repo will download and format the weather data from the NOAA official stations dating back to the 1970's. 

downloader.py - downloads all the compressed folders of data
untar.sh - uncompresses all the compressed folders
rename.sh - renames all the txt files to csv files
toMongo.py - reads and formats csv files, then sends the data to a MongoDB instance
