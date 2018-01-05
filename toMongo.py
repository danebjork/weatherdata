import datetime as dt
import pymongo
import csv
import sys
import pandas
import json


weather_labels = ['station',
            'date',
            'time',
            'visibility',
            'drytemp',
            'wettemp',
            'dewtemp',
            'humidity',
            'windspeed',
            'winddirection',
            'pressure',
            'seapressure',
            'precipitation',
            'minute',
            'hour',
            'day',
            'month',
            'year'
           ]

with open("/Users/danebjork/weather/allstations.txt") as json_data:
    all_stations = json.load(json_data)

client = pymongo.MongoClient()
db = client['noaa']

def oldformatter(row):
    try:
        currRow = row
        date = str(row[1])
        time = str(row[2])
        day = date[6:]
        month = date[4:6]
        year = date[0:4]
        try:
            station = all_stations[curr_stations[int(row[0])]]
        except:
            station = {'long': 0, 'lat':0, 'tz': 0, 'height':0}
            
        if len(time) == 1:
            hour = "00"
            minute = "0" + time
        elif len(time) == 2:
            hour = "0"
            minute = time
        elif len(time) == 3:
            hour = time[0]
            minute = time[1:]
        else:
            hour = time[0:2]
            minute = time[2:]
        try:
            tz = int(station['tz'])
        except:
            tz = 0
        timestamp = dt.datetime(int(year), int(month), int(day)) + dt.timedelta(hours=int(hour)) + dt.timedelta(minutes=int(minute)) + dt.timedelta(hours=tz)
        
        precip = 0.0
        try:
            precip = float(row[20])
        except:
            if row[20] == 'T':
                precip = .001
            else:
                precip = 0.0
        return pandas.Series({'timestamp':timestamp,
                              'precip':precip,
                             'long': station['long'],
                             'lat':station['lat'],
                             'height':station['height']
                             })
    except:
        return pandas.Series({'timestamp':0,
                      'precip':0,
                     'long': 0,
                     'lat':0,
                     'height':0
                     })



def pushOld(directory, filename):
    # Get a mapping from station ID to station Global Label
    print("Reading File")
    # Read in the file
    df = pandas.read_csv(filename, delimiter=',', encoding='iso8859_2', error_bad_lines=False)
    try:
        del df['Unnamed: 21']
    except:
        pass
    #change types to int
    for i in [0, 8,10, 9,11,12,13,16,18]:
        df[df.columns[i]] = pandas.to_numeric(df[df.columns[i]], errors='coerce')
    # rename for manipulation
    df.columns = ['station','date','time','c3','c4','c5','c6','c7', 
                  'drytemp', 'dewtemp', 'wettemp', 'humid', 'wspeed','wdir', 
                  'c14','c15',  'pressure', 'c17', 'seapressure', 'c19', 'precip_old']
    df = df[df.station.notnull()] #remove empty station
    df = df[df.station >= 100]
    df = df[df.station <= 99901]
    df = df[df.date.notnull()] #remove empty date
    df = df[df.time.notnull()] #remove empty time
    df = df[df.drytemp.notnull()] #remove empty temperature
    df[['height', 'lat', 'long', 'precip','utctime']] = df.apply(oldformatter, axis=1)
    indexes1 = [0, 8,10, 9,11,12,13,16,18,21,22,23,24,25]
    toRemove = []
    for i in range(len(df.columns)):
        if i not in indexes1:
            toRemove.append(df.columns[i])
    for rem in toRemove:
        df = df.drop(rem, 1)
    return df

def newformatter(row):
    try:
        currRow = row
        date = str(row[1])
        time = str(row[2])
        day = date[6:]
        month = date[4:6]
        year = date[0:4]

        try:
            station = all_stations[curr_stations[int(row[0])]]
        except:
            station = {'long': 0, 'lat':0, 'tz': 0, 'height':0}
        if len(time) == 1:
            hour = "00"
            minute = "0" + time
        elif len(time) == 2:
            hour = "0"
            minute = time
        elif len(time) == 3:
            hour = time[0]
            minute = time[1:]
        else:
            hour = time[0:2]
            minute = time[2:]
        try:
            tz = int(station['tz'])
        except:
            print("Unexpected error:", sys.exc_info()[0])
            currRow = row
            tz = 0
        timestamp = dt.datetime(int(year), int(month), int(day)) + dt.timedelta(hours=int(hour)) + dt.timedelta(minutes=int(minute)) + dt.timedelta(hours=tz)

        precip = 0.0
        try:
            precip = float(row[40])
        except:
            if row[20] == 'T':
                precip = .001
            else:
                precip = 0.0
        toReturn = pandas.Series({'timestamp':timestamp,
                              'precip':precip,
                             'long': station['long'],
                             'lat':station['lat'],
                             'height':station['height']
                             })
        return toReturn
    except:
        print("ERROR!")
        return pandas.Series({'timestamp':0,
              'precip':0,
             'long': 0,
             'lat':0,
             'height':0})


def pushNew(directory, filename):
    # Read in the file
    df = pandas.read_csv(filename, delimiter=',', encoding='iso8859_2', error_bad_lines=False)
    for i in [0, 8,10, 9,11,12,13,16,18]:
        df[df.columns[i]] = pandas.to_numeric(df[df.columns[i]], errors='coerce')
    # rename for manipulation
    df.columns = ['station','date','time','c3','c4','c5','c6','c7', 'c8','c9',
                  'drytemp','c11', 'c12', 'c13', 'wettemp', 'c15', 'c16', 'c17', 'dewtemp',  'c19', 'c20', 'c21', 
                  'humid', 'c23', 'wspeed', 'c25','wdir', 'c27', 'c28', 'c29', 
                  'pressure','c31','c32','c33','c34','c35', 'seapressure', 'c37','c38','c39',
                  'precip_old','c41','c42','c43']
    df = df[df.station.notnull()] #remove empty station
    df = df[df.station >= 100]
    df = df[df.station <= 99901]
    df = df[df.date.notnull()] #remove empty date
    df = df[df.time.notnull()] #remove empty time
    df = df[df.drytemp.notnull()] #remove empty temperature
    df[['height', 'lat', 'long', 'precip','utctime']] = df.apply(newformatter, axis=1)
    indexes1 = [0, 10, 14, 18, 22, 24, 26, 30, 36, 44, 45, 46, 47, 48]
    toRemove = []
    for i in range(len(df.columns)):
        if i not in indexes1:
            toRemove.append(df.columns[i])
    for rem in toRemove:
        df = df.drop(rem, 1)

    return df


try:
    db['hourly'].drop()
    print("Collection Dropped")
except:
    print("Nothing Dropped")

curr_stations = {}
fails = []
base = "/Users/danebjork/weather/"
for year in range(1996, 2018):
    for month in range(1, 13):
        directory = str(year) + str(month).zfill(2)
        filename = "/Users/danebjork/weather/{0}/{0}hourly.txt".format(directory)
        if (year == 1996 and month > 6) or (year > 1996 and year < 2007) or (year == 2007 and month < 5):
            try:
                print(filename)
                curr_stations = {}
                ds = pandas.read_csv("/Users/danebjork/weather/{}/station.txt".format(directory), delimiter='|', encoding='iso8859_2', error_bad_lines=False)
                for row in ds.iterrows():
                    curr_stations[row[1][0]] = row[1][2]
                db['hourly'].insert_many(pushOld(directory, filename).to_dict('records'))
                print("Pushed!!!")
            except:
                print("Unexpected error MAIN:", sys.exc_info()[0])
                print("FAIL!")
                fails.append(filename)
        elif(year == 2007 and month >= 5) or (year < 2017 and year > 2007) or (year == 2017 and month <= 3):
            try:
                print(filename)
                curr_stations = {}
                ds = pandas.read_csv("/Users/danebjork/weather/{0}/{0}station.txt".format(directory), delimiter='|', encoding='iso8859_2', error_bad_lines=False)
                for row in ds.iterrows():
                    curr_stations[row[1][0]] = row[1][2]
                db['hourly'].insert_many(pushNew(directory, filename).to_dict('records'))
                print("Pushed!!!")
            except:
                print("FAIL:", sys.exc_info()[0])
                fails.append(filename)
                
print(fails)
