import os
import json
import requests
import sqlite3
import urllib
import matplotlib.pyplot as plt

def open_database(db):
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    return cur, conn

def load_json(filename):
    try:
        fhand = open(filename, "r")
        string = fhand.read()    
        fhand.close()
        d = json.loads(string)
        fhand.close()
    except:
        d = {}
    return d

def read_location_data(file):
    with open(file, 'r') as json_file:
        location_data = json.load(json_file)
    return location_data

def write_json(filename, dict): 
    with open(filename, "w") as fhand:
        json.dump(dict,fhand)


def get_api(url, params):
    try:
        r = requests.get(url, params = params)
        return r.json()
    except:
        print("Cannot find API")
        return None


def cache_elevation_data(location_data, elevation_file):
    elevation_d = {}
    base_url = "https://api.opentopdata.org/v1/ned10m"
    for state, cities in location_data.items():
        for city, coordinates in cities.items():
            lat = round(coordinates[0], 3)
            lon = round(coordinates[1], 3)
            params = {"locations": f"{lat},{lon}"}
            #TODO: dict for locations and lat//lon
            try:
                result = get_api(f"{base_url}?locations={lat},{lon}", params=None)
                if result:
                    elevation_d[city] = result
                else:
                    print(f"{city}: No elevation data found")
            except:
                print(f"{city}: Failed to fetch elevation data")
    write_json(elevation_file, elevation_d)

def process_elevation_data(elevation_file_r, elevation_file):
    elevation_r = load_json(elevation_file_r)
    elevation_d = {}
    for location in elevation_r:
        elevation = elevation_r[location].get("results", {}).get("elevation",None)
        if elevation is not None:
            elevation_d[location] = elevation
    write_json(elevation_file, elevation_d)
    return elevation_d

def make_elevation_table(filename, cur, conn):
    elevation = load_json(filename)
    cur.execute('''CREATE TABLE IF NOT EXISTS Elevation 
                id INTEGER PRIMARY KEY,
                location_name TEXT,
                elevation FLOAT)''')
    lst2 = []
    for location in elevation:
        location_n = location
        elevation_val = elevation[location]
        lst2.append(location_n, elevation_val)

    cur.execute('''SELECT MAX(id) FROM Elevation''')
    last_id = cur.fetchone()[0]
    if last_id is None:
        last_id = 0

    start_id = last_id + 1
    count = 0
    for i in range(start_id, start_id + 25):
        if count >= 25:
            break
        cur.execute('''INSERT OR IGNORE INTO Elevation
                    (id, location_name, elevation) VALUES (?,?,?)''',
                    (i, lst2[i][0], lst2[i][1]))
        count += 1
    conn.commit()


def main():
    location_file = "location_data.json"
    elevation_file_r = "elevation_data_r.json"
    elevation_file = "elevation_data.json"
    cur, conn = open_database("Mental_Health.db")
    location_data = read_location_data(location_file)
    cache_elevation_data(location_data, elevation_file_r)
    process_elevation_data(elevation_file_r, elevation_file)
    make_elevation_table(elevation_file, cur, conn)
    conn.close()

if __name__ == "__main__":
    main()
