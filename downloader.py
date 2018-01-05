import urllib
base_url = "https://www.ncdc.noaa.gov/orders/qclcd/"
test = 7
file_type1 = ".tar.gz"
file_type2 = ".zip"
stringTest = "%02d" % (test)

for year in range(1996, 2018):
    for month in range(1, 13):
        url_end = "bad"
        if year > 2007 or (year == 2007 and month >= 5):
            url_end = "QCLCD" + str(year) + "%02d" % (month) + ".zip"
            print url_end
            urllib.urlretrieve(base_url + url_end, url_end[5:])
        else:
            url_end = str(year) + "%02d" % (month) + ".tar.gz"
            print url_end
            urllib.urlretrieve(base_url + url_end, url_end)
