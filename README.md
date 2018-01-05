# NOAA Weather Collection

This repo will download and format the weather data from the NOAA official stations dating back to the 1970's. 

Run these scripts in order to download NOAA's weather data. (It will take a whiel. Last time I ran it, it took over 3 days to download, process, and write the data to the database.)

1. downloader.py - downloads all the compressed folders of data
2. untar.sh - uncompresses all the compressed folders
3. rename.sh - renames all the txt files to csv files
4. toMongo.py - reads and formats csv files, then sends the data to a MongoDB instance
