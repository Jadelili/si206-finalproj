import os
import json
import requests
import sqlite3
import urllib
import matplotlib.pyplot as plt

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


def write_json(filename, dict): 
    with open(filename, "w") as fhand:
        fhand.write(json.dumps(dict))


def get_api(url, params):
    try:
        r = requests.get(url, params)
        return r.json()
    except:
        print("Cannot find API")
        return None


def cache_health_data(filename):
    health_list = []
    for i in range(2000,55000,10000):
        base_url = 'https://chronicdata.cdc.gov/resource/cj8b-94cj.json'
        where_clause = f'totalpopulation > {i}'
        url = f"{base_url}?$where={urllib.parse.quote(where_clause)}"
        group_result = get_api(url, None)
        for dic in group_result:
            health_list.append(dic)
    write_json(filename, health_list)


def get_lat(f_r, f_w):
    loc_list = load_json(f_r)
    state_d = {}
    for region_dic in loc_list:
        state = region_dic["stateabbr"]
        city = region_dic["placename"]
        coor_b = region_dic["geolocation"]["coordinates"]
        pop = int(region_dic["totalpopulation"])
        
        d = {}
        coor = (coor_b[1], coor_b[0])
        d[city] = (pop, coor)
        if state not in state_d.keys():
            state_d[state] = d
        else:
            state_d[state].update(d)
    
    three_city_d = {}
    for state in state_d:
        new_d = {}
        s_city = sorted(state_d[state].items(), key = lambda x:x[1], reverse=True)
        new_d[s_city[0][0]] = s_city[0][1][1]
        if len(s_city) > 1:
            new_d[s_city[1][0]] = s_city[1][1][1]
            new_d[s_city[2][0]] = s_city[2][1][1]
        three_city_d[state] = new_d
    write_json(f_w, three_city_d)
    # print(three_city_d)
    return three_city_d


def cache_weather_data(three_city_d, weatherfile):
    weather_d = {}
    base = "https://history.openweathermap.org/data/2.5/aggregated/year"
    for state in three_city_d:
        d = {}
        for city in three_city_d[state]:
            dic = {"lat":three_city_d[state][city][0], "lon":three_city_d[state][city][1], "appid":"0ed702778efe831f0e3d546dc34eece3"}
            result = get_api(base, dic)
            d[city] = result
            if state not in weather_d.keys():
                weather_d[state] = d
            else:
                weather_d[state].update(d)
    write_json(weatherfile, weather_d)


def cache_elevation_data(three_city_d, elevationfile_r):
    elevation_d = {}
    base = "https://api.open-meteo.com/v1/elevation"
    for state in three_city_d:
        d = {}
        for city in three_city_d[state]:
            dic = {"latitude":three_city_d[state][city][0], "longitude":three_city_d[state][city][1]}
            result = get_api(base, dic)
            d[city] = result
            if state not in elevation_d.keys():
                elevation_d[state] = d
            else:
                elevation_d[state].update(d)
    write_json(elevationfile_r, elevation_d)

def process_weather_data(weatherfile_r, weatherfile):
    weather_r = load_json(weatherfile_r)
    lst = []
    weather_d = {}
    for state in weather_r:
        city_d = {}
        for city in weather_r[state]:
            d = {}
            total_t_median = 0
            total_ps_median = 0
            total_h_median = 0
            total_c_median = 0 
            count = 0
            try: 
                for i in weather_r[state][city]["result"]:
                    if i["month"] in [11,12,1,2,3]: 
                        count += 1
                        total_t_median += i["temp"]["median"]
                        total_ps_median += i["pressure"]["median"]
                        total_h_median += i["humidity"]["median"]
                        total_c_median += i["clouds"]["median"]
                    t_avg = total_t_median / count
                    ps_avg = total_ps_median / count
                    hum_avg = total_h_median / count
                    c_avg = total_c_median / count

                    d["temp_medium"] = t_avg
                    d["pressure_medium"] = ps_avg
                    d["humidity_medium"] = hum_avg
                    d["clouds_medium"] = c_avg

                    if city not in city_d.keys():
                        city_d[city] = d
                    elif state == "ME" and city == "Portland":
                        city_d["Portland"] = d
                    elif state == "WV" and city == "Charleston":
                        city_d["Charleston"]= d
                    elif state == "MD" and city == "Columbia":
                        city_d["Columbia"] = d
                
            except:
                continue

        if state not in list(weather_d.keys()):
            weather_d[state] = city_d
        else:
            weather_d[state].update(city_d)
    # print(weather_d)
    write_json(weatherfile, weather_d)
    return weather_d


def process_health_data(healthfile_r, healthfile, three_city_d):
    health_r = load_json(healthfile_r)
    health_d = {}
    count = 0
    for h_city in health_r:
        for i in three_city_d:
            city_d = {}
            # if h_city["stateabbr"] == i and h_city["placename"] in three_city_d[i].keys():
            if h_city["stateabbr"] == i and h_city["placename"] in three_city_d[i].keys():
                hd = {}
                if "depression_crudeprev" in h_city.keys():
                    hd["depression"] = float(h_city["depression_crudeprev"])
                else:
                    hd["depression"] = "null"

                if "mhlth_crudeprev" in h_city.keys():
                    hd["mh_not_good"] = float(h_city["mhlth_crudeprev"])
                else:
                    hd["mh_not_good"] = "null"

                if "sleep_crudeprev" in h_city.keys():
                    hd["sleep_less_7"] = float(h_city["sleep_crudeprev"])
                else:
                    hd["sleep_less_7"] = "null"

                if "lpa_crudeprev" in h_city.keys():
                    hd["no_leis_phy_act"] = float(h_city["lpa_crudeprev"])
                else:
                    hd["no_leis_phy_act"] = "null"

                if h_city["placename"] not in city_d.keys():
                    city_d[h_city["placename"]] = hd
                elif h_city["stateabbr"] == "ME" and h_city["placename"] == "Portland":
                    city_d["Portland"] = hd
                elif h_city["stateabbr"] == "WV" and h_city["placename"] == "Charleston":
                    city_d["Charleston"] = hd
                elif h_city["stateabbr"] == "MD" and h_city["placename"] == "Columbia":
                    city_d["Columbia"] = hd

                if h_city["stateabbr"] not in health_d.keys():
                    health_d[h_city["stateabbr"]] = city_d
                else:
                    health_d[h_city["stateabbr"]].update(city_d)
    # print(health_d)
    write_json(healthfile, health_d)
    return health_d


def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + db_name)
    cur = conn.cursor()
    return cur, conn


def make_state_city_table(filename, cur, conn):
    loc = load_json(filename)
    cur.execute("CREATE TABLE IF NOT EXISTS Location (city_id INTEGER PRIMARY KEY, city_name TEXT, state_abbr TEXT)")
    for state in loc:
        for city in loc[state]:
            cur.execute('SELECT id FROM Weather WHERE city_name = ?', (city, ))
            c_id = cur.fetchone()
            cur.execute("INSERT OR IGNORE INTO Location (city_id, city_name, state_abbr) VALUES (?,?,?)", (c_id[0], city, state))
    conn.commit()


def make_state_table(filename, cur, conn):
    loc = load_json(filename)
    cur.execute("CREATE TABLE IF NOT EXISTS State (id INTEGER PRIMARY KEY, state_abbr TEXT)")
    
    # get num states already in table
    cur.execute("SELECT COUNT(*) FROM State")
    num_states_added = cur.fetchone()[0]
    
    # counter for items added in this run
    items_added = 0
    
    for state in loc:
        # Add states tht haven't been added
        if items_added >= 25 or num_states_added >= len(loc):
            break
            
        cur.execute("SELECT id FROM State WHERE state_abbr = ?", (state,))
        state_id = cur.fetchone()
        if state_id is None:
            cur.execute("INSERT INTO State (id, state_abbr) VALUES (?,?)", (num_states_added, state))
            num_states_added += 1
            items_added += 1
            
    conn.commit()



def make_weather_table(filename, cur, conn):
    weather = load_json(filename)

    # create Weather table if it doesn't already exist
    cur.execute('''CREATE TABLE IF NOT EXISTS Weather (id INTEGER PRIMARY KEY, city_name TEXT, state_id INTEGER, 
    temp FLOAT, pressure FLOAT, humidity FLOAT, clouds FLOAT)''')

    # From State table: get state ids
    state_ids = {}
    cur.execute("SELECT id, state_abbr FROM State")
    for row in cur.fetchall():
        state_ids[row[1]] = row[0]

    # Get cities that have been added already
    cur.execute("SELECT city_name FROM Weather")
    cities_added = set(row[0] for row in cur.fetchall())

    # Add data to Weather table for next 25 items
    items_added = 0
    for state in weather:
        for city in weather[state]:
            if items_added >= 25:
                break
            if city in cities_added:
                continue
            temp = round(weather[state][city]["temp_medium"], 2)
            ps = round(weather[state][city]["pressure_medium"], 2)
            hum = round(weather[state][city]["humidity_medium"], 2)
            clouds = round(weather[state][city]["clouds_medium"], 2)
            cur.execute('''SELECT COUNT(*) FROM Weather WHERE temp = ? AND pressure = ? AND humidity = ? AND clouds = ?''',
                        (temp, ps, hum, clouds))
            if cur.fetchone()[0] == 0:
                cur.execute("INSERT INTO Weather (city_name, state_id, temp, pressure, humidity, clouds) VALUES (?, ?, ?, ?, ?, ?)",
                            (city, state_ids[state], temp, ps, hum, clouds))
                items_added += 1
                cities_added.add(city)
            else:
                print(f"Skipping duplicate data for {city}, {state}")
        if items_added >= 25:
            break
    conn.commit()


def make_health_table(filename, cur, conn):
    health = load_json(filename)
    
    # create Health table if it doesn't exist
    cur.execute('''CREATE TABLE IF NOT EXISTS Health (
               city_id INTEGER PRIMARY KEY,
               city_name TEXT,
               state_id INTEGER,
               depression FLOAT,
               mh_not_good FLOAT,
               sleep_less_7 FLOAT,
               no_leis_phy_act FLOAT,
               FOREIGN KEY (city_id) REFERENCES Weather (id)
             )''')
    # From weather table: get cities and ids that have already been added
    cur.execute("SELECT city_name, id FROM Weather")
    cities_added = dict(row for row in cur.fetchall())

    # Row counter per run
    items_added = 0

    # Loop through health dict
    for state in health:
        for city in health[state]:
            if items_added >= 25:
                break

            # Check if the city has already been added
            if city not in cities_added:
                continue

            # get ids and data for current city
            city_id = cities_added[city]
            state_id = cur.execute("SELECT state_id FROM Weather WHERE id = ?", (city_id,)).fetchone()[0]
            health_data = health[state][city]

            # check if city_id already corresponds to a row in Health table
            existing_row = cur.execute("SELECT * FROM Health WHERE city_id = ?", (city_id,)).fetchone()
            if existing_row:
                # Update current row with health data
                cur.execute('''UPDATE Health SET city_name = ?, state_id = ?, depression = ?, mh_not_good = ?, sleep_less_7 = ?, no_leis_phy_act = ?
                            WHERE city_id = ?''',
                            (city, state_id, health_data["depression"], health_data["mh_not_good"], health_data["sleep_less_7"], health_data["no_leis_phy_act"], city_id))
            else:
                # Insert a new row with the health data for the current city
                cur.execute('''INSERT INTO Health (city_id, city_name, state_id, depression, mh_not_good, sleep_less_7, no_leis_phy_act) 
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (city_id, city, state_id, health_data["depression"], health_data["mh_not_good"], health_data["sleep_less_7"], health_data["no_leis_phy_act"]))
        if items_added >= 25:
            break
    conn.commit()
   
def make_elevation_table(filename, cur, conn):
    elevation_file = load_json(filename)

    # create Elevation table if it doesn't already exist
    cur.execute('''CREATE TABLE IF NOT EXISTS Elevation (id INTEGER PRIMARY KEY, city_name TEXT, state_id INTEGER, 
    elevation FLOAT)''')

    # From State table: get state ids
    state_ids = {}
    cur.execute("SELECT id, state_abbr FROM State")
    for row in cur.fetchall():
        state_ids[row[1]] = row[0]

    # Get cities that have been added already
    cur.execute("SELECT city_name FROM Elevation")
    cities_added = set(row[0] for row in cur.fetchall())

    # Add data to Elevation table for next 25 items
    items_added = 0
    for state in elevation_file:
        for city in elevation_file[state]:
            if items_added >= 25:
                break
            if city in cities_added:
                continue
            elevation = elevation_file[state][city]["elevation"]
            cur.execute('''SELECT COUNT(*) FROM Elevation WHERE elevation = ?''',
                        (elevation))
            if cur.fetchone()[0] == 0:
                state_id = state_ids[state]
                city_name = city
                city_elevation = elevation[0]
                cur.execute("INSERT INTO Elevation (city_name, state_id, elevation) VALUES (?, ?, ?)",
                            (city, state_id, city_elevation))
                items_added += 1
                cities_added.add(city)
            else:
                print(f"Skipping duplicate data for {city}, {state}")
        if items_added >= 25:
            break
    conn.commit()

def main():
    three_city_d = get_lat("health_data_r.json", "coordinate.json")
    ###cache_health_data("health_data_r.json")
    ###cache_weather_data(three_city_d, "weather_data_raw.json")
    ###cache_elevation_data(three_city_d, "elevation_data.json")
    process_weather_data("weather_data_raw.json", "weather_data.json")
    process_health_data("health_data_r.json", "health_data.json", three_city_d)
    cur, conn = open_database("Mental_health.db")
    make_state_table("weather_data.json", cur, conn)
    make_weather_table("weather_data.json", cur, conn)
    make_health_table("health_data.json", cur, conn)
    make_elevation_table("elevation_data_raw.json", cur, conn)
    ### make_state_city_table("location_data.json", cur, conn)

if __name__ == "__main__":
    main()
