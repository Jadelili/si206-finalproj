import os
import json
import requests
import sqlite3
import urllib
import csv
import numpy as np
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
    return three_city_d


def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + db_name)
    cur = conn.cursor()
    return cur, conn


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

                if h_city["stateabbr"] == "WV" and h_city["placename"] == "Charleston":
                    city_d["Charleston_WV"] = hd
                elif h_city["placename"] not in city_d.keys():
                    city_d[h_city["placename"]] = hd
                elif h_city["stateabbr"] == "ME" and h_city["placename"] == "Portland":
                    city_d["Portland"] = hd
                elif h_city["stateabbr"] == "MD" and h_city["placename"] == "Columbia":
                    city_d["Columbia"] = hd

                if h_city["stateabbr"] not in health_d.keys():
                    health_d[h_city["stateabbr"]] = city_d
                else:
                    health_d[h_city["stateabbr"]].update(city_d)
    write_json(healthfile, health_d)
    return health_d


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
                cur.execute('''INSERT OR IGNORE INTO Health (city_id, city_name, state_id, depression, mh_not_good, sleep_less_7, no_leis_phy_act) 
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (city_id, city, state_id, health_data["depression"], health_data["mh_not_good"], health_data["sleep_less_7"], health_data["no_leis_phy_act"]))
    conn.commit()


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

                    if state == "WV" and city == "Charleston":
                        city_d["Charleston_WV"]= d
                    if city not in city_d.keys():
                        city_d[city] = d
                    elif state == "ME" and city == "Portland":
                        city_d["Portland"] = d
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
    cities_added = [row[0] for row in cur.fetchall()]
    # Add data to Weather table for next 25 items
    cur.execute("SELECT MAX(id) FROM Weather")
    max_city_id = cur.fetchone()[0] or 0  # Use 0 if there are no existing rows
    items_added = 0
    for state in weather:
        for city in weather[state]:
            if items_added >= 25:
                break
            if city in cities_added:
                continue
            state_id = state_ids[state]
            temp = round(weather[state][city]["temp_medium"] * 1.8 - 459.67, 2) 
            ps = round(weather[state][city]["pressure_medium"], 2)
            hum = round(weather[state][city]["humidity_medium"], 2)
            clouds = round(weather[state][city]["clouds_medium"], 2)
            cur.execute('''SELECT COUNT(*) FROM Weather WHERE temp = ? AND pressure = ? AND humidity = ? AND clouds = ?''',
                        (temp, ps, hum, clouds))
            if cur.fetchone()[0] == 0:
                city_id = max_city_id + 1 
                cur.execute("INSERT OR IGNORE INTO Weather (id, city_name, state_id, temp, pressure, humidity, clouds) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (city_id, city, state_id, temp, ps, hum, clouds))
                items_added += 1
                cities_added.append(city)
                max_city_id += 1
            else:
                print(f"Skipping duplicate data for {city}, {state}")
    conn.commit()


def cache_elevation_data(three_city_d, elevationfile_r):
    elevation_d = {}
    base = "https://api.open-meteo.com/v1/elevation"
    for state in three_city_d:
        d = {}
        for city in three_city_d[state]:
            dic = {"latitude":three_city_d[state][city][0], "longitude":three_city_d[state][city][1]}
            result = get_api(base, dic)
            elevation = result['elevation'][0]
            d[city] = elevation
            if state not in elevation_d.keys():
                elevation_d[state] = d
            else:
                elevation_d[state].update(d)
    write_json(elevationfile_r, elevation_d)


def make_elevation_table(filename, cur, conn):
    elevation_file = load_json(filename)
    # create Elevation table if it doesn't already exist
    cur.execute('''CREATE TABLE IF NOT EXISTS Elevation (city_id INTEGER PRIMARY KEY, city_name TEXT, state_id INTEGER, 
    elevation FLOAT)''')
    # Get cities that have been added to Weather already
    cur.execute("SELECT city_name FROM Weather")
    cities_added = [row[0] for row in cur.fetchall()]
    cur.execute("SELECT MAX(city_id) FROM Elevation")
    max_city_id = cur.fetchone()[0] or 0  # Use 0 if there are no existing rows
    items_added = 0
    # Set the starting index for the loop to the next ID after the maximum
    next_id = max_city_id + 1
    # Loop through the cities in the elevation file starting at next_id
    for state in elevation_file:
        for city in elevation_file[state]:
            if items_added >= 25:
                break
            if city not in cities_added:
                continue
            elevation = elevation_file[state][city]
            cur.execute('SELECT id FROM Weather WHERE city_name = ?', (city, ))
            c_id = cur.fetchone()[0]
            cur.execute('SELECT id FROM State WHERE state_abbr = ?', (state, ))
            state_id = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM Elevation WHERE city_name = ?', (city,))
            if cur.fetchone()[0] > 0:
                continue
            cur.execute("INSERT OR IGNORE INTO Elevation (city_id, city_name, state_id, elevation) VALUES (?, ?, ?, ?)",
                (next_id, city, state_id, elevation))
            items_added += 1
            cities_added.append(city)
            next_id += 1
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
            cur.execute("INSERT OR IGNORE INTO State (id, state_abbr) VALUES (?,?)", (num_states_added, state))
            num_states_added += 1
            items_added += 1
        
    conn.commit()


def cache_sun_data(three_city_d, sunfile_r):
    sun_d = {}
    base = "https://archive-api.open-meteo.com/v1/archive"
    for state in three_city_d:
        d = {}
        for city in three_city_d[state]:
            dic = {"latitude":three_city_d[state][city][0], "longitude":three_city_d[state][city][1], "start_date":"2020-11-01", "end_date":"2021-03-31", 
            "daily":"temperature_2m_mean,apparent_temperature_mean,sunrise,sunset,shortwave_radiation_sum,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours",
            "timezone":"America/Los_Angeles","temperature_unit":"fahrenheit","precipitation_unit":"inch"}
            result = get_api(base, dic)
            d[city] = result
            if state not in sun_d.keys():
                sun_d[state] = d
            else:
                sun_d[state].update(d)
    write_json(sunfile_r, sun_d)


def process_sun_data(sunfile_r, sunfile):
    sun_r = load_json(sunfile_r)
    lst = []
    sun_d = {}
    for state in sun_r:
        city_d = {}
        for city in sun_r[state]:
            d = {}
            day_num = len(sun_r[state][city]["daily"]["time"])

            duration = 0
            temp = 0
            app_temp = 0
            rad = 0
            prec = 0
            rain = 0
            snow = 0
            prec_h = 0
            for i in range(day_num):
                temp += sun_r[state][city]["daily"]["temperature_2m_mean"][i]
                app_temp += sun_r[state][city]["daily"]["apparent_temperature_mean"][i]
                rad += sun_r[state][city]["daily"]["shortwave_radiation_sum"][i]
                prec += sun_r[state][city]["daily"]["precipitation_sum"][i]
                rain += sun_r[state][city]["daily"]["rain_sum"][i]
                snow += sun_r[state][city]["daily"]["snowfall_sum"][i]
                prec_h += sun_r[state][city]["daily"]["precipitation_hours"][i]
                # print(sun_r[state][city]["daily"]["sunset"][i], sun_r[state][city]["daily"]["sunrise"][i])
                sunrise = int(sun_r[state][city]["daily"]["sunrise"][i][-4:-3]) * 60 + int(sun_r[state][city]["daily"]["sunrise"][i][-2:])
                sunset = int(sun_r[state][city]["daily"]["sunset"][i][-5:-3]) * 60 + int(sun_r[state][city]["daily"]["sunset"][i][-2:])
                duration_day = sunset - sunrise
                duration += duration_day
            
            d["temp"] = round(temp / day_num, 2)
            d["app_temp"] = round(app_temp / day_num, 2)
            d["rad"] = round(rad / day_num, 2)
            d["prec"] = round(prec / day_num, 2)
            d["rain"] = round(rain / day_num, 2)
            d["snow"] = round(snow / day_num, 2)
            d["prec_hours"] = round(prec_h / day_num, 2)
            d["sunlight_hours"] = round(duration / day_num, 2)

            if state == "WV" and city == "Charleston":
                city_d["Charleston_WV"] = d
            elif city not in city_d.keys():
                city_d[city] = d
            elif state == "ME" and city == "Portland":
                city_d["Portland"] = d
            elif state == "MD" and city == "Columbia":
                city_d["Columbia"] = d
        
        if state not in list(sun_d.keys()):
            sun_d[state] = city_d
        else:
            sun_d[state].update(city_d)
    write_json(sunfile, sun_d)
    return sun_d


def make_sun_table(filename, cur, conn):
    sun = load_json(filename)
    
    # create Health table if it doesn't exist
    cur.execute('''CREATE TABLE IF NOT EXISTS Sun (
               city_id INTEGER PRIMARY KEY,
               city_name TEXT,
               state_id INTEGER,
               temp FLOAT,
               app_temp FLOAT,
               rad FLOAT,
               prec FLOAT,
               rain FLOAT,
               snow FLOAT,
               prec_hours FLOAT,
               sunlight_hours INTEGER,
               FOREIGN KEY (city_id) REFERENCES Weather (id)
             )''')
    
    # From Weather table: get cities and ids that have already been added
    cur.execute("SELECT city_name, id FROM Weather")
    cities_added = dict(row for row in cur.fetchall())
    # Row counter per run
    items_added = 0
    # Loop through sun dict
    for state in sun:
        for city in sun[state]:
            if items_added >= 25:
                break
            # Check if the city has already been added
            if city not in cities_added:
                continue
            # get ids and data for current city
            city_id = cities_added[city]
            state_id = cur.execute("SELECT state_id FROM Weather WHERE id = ?", (city_id,)).fetchone()[0]
            sun_data = sun[state][city]
            # check if city_id already corresponds to a row in Sun table
            existing_row = cur.execute("SELECT * FROM Sun WHERE city_id = ?", (city_id,)).fetchone()
            if existing_row:
                # Update current row with sun data
                cur.execute('''UPDATE Sun SET city_name = ?, state_id = ?, temp = ?, app_temp = ?, rad = ?, prec = ?, rain = ?, snow = ?, prec_hours = ?, sunlight_hours = ?
                  WHERE city_id = ?''', (city, state_id, sun_data["temp"], sun_data["app_temp"], sun_data["rad"], sun_data["prec"], sun_data["rain"], sun_data["snow"], 
                                         sun_data["prec_hours"], sun_data["sunlight_hours"], city_id))
            else:
                # Insert a new row with the health data for the current city
                cur.execute('''INSERT OR IGNORE INTO Sun (city_id, city_name, state_id, temp, app_temp, rad, prec, rain, snow, prec_hours, sunlight_hours) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (city_id, city, state_id, sun_data["temp"], sun_data["app_temp"], sun_data["rad"], sun_data["prec"], sun_data["rain"], sun_data["snow"], 
                             sun_data["prec_hours"], sun_data["sunlight_hours"]))
    conn.commit()


# def join_tables(cur, conn):
#     cur.execute('''CREATE TABLE IF NOT EXISTS CombinedData 
#                     (id INTEGER PRIMARY KEY, city_name TEXT,
#                     elevation FLOAT, temp FLOAT, humidity FLOAT, pressure FLOAT, clouds FLOAT, 
#                     precipitation FLOAT, sunlight FLOAT, depression FLOAT, lack_of_sleep FLOAT, 
#                     physical_activity FLOAT)''')
   
#     cur.execute('''SELECT Weather.id, Weather.city_name, Elevation.elevation, 
#                     Weather.temp, Weather.humidity, Weather.pressure, Weather.clouds, 
#                     Weather.precipitation, Sun.sunlight, Health.depression, Health.lack_of_sleep, 
#                     Health.physical_activity 
#                     FROM Weather
#                     JOIN Elevation ON Weather.id=Elevation.id 
#                     JOIN Sun ON Weather.id=Sun.id 
#                     JOIN Health ON Weather.id=Health.id''')

#     results = cur.fetchall()

#     cur.executemany('''INSERT INTO CombinedData 
#                         (id, city_name, elevation, temp, humidity, pressure, clouds, 
#                         precipitation, sunlight, depression, lack_of_sleep, physical_activity) 
#                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', results)

#     conn.commit()


# def sun_depression(cur, filename):
#     cur.execute('''SELECT Sun.id, Sun.sunlight_hours, Weather.clouds, Health.depression 
#                     FROM Sun 
#                     JOIN Weather ON Sun.id = Weather.id 
#                     JOIN Health ON Sun.id = Health.id''')
#     data = cur.fetchall()

#     # Scale the data
#     scaled_data = []
#     for row in data:
#         scaled_row = [(float(val) - min(row)) / (max(row) - min(row)) for val in row]
#         scaled_data.append(scaled_row)

#     # Calculate the correlation matrix
#     correlation_matrix = np.corrcoef(scaled_data, rowvar=False)

#     # Write the correlation matrix to a CSV file
#     with open('correlation_matrix.csv', 'w', newline='') as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerows(correlation_matrix)




# def calculations_two(cur,conn):
#     cur.execute('''SELECT c''')

# def make_plot_one(cur, conn):

# def make_plot_two(cur, conn):


def main():
    three_city_d = get_lat("health_data_r.json", "coordinate.json")
    ###cache_health_data("health_data_r.json")
    ###cache_weather_data(three_city_d, "weather_data_raw.json")
    ###cache_elevation_data(three_city_d, "elevation_data.json")
    ###cache_sun_data(three_city_d, "sun_data_r.json")
    process_weather_data("weather_data_raw.json", "weather_data.json")
    process_health_data("health_data_r.json", "health_data.json", three_city_d)
    process_sun_data("sun_data_r.json", "sun_data.json")
    cur, conn = open_database("Mental_health.db")
    make_state_table("weather_data.json", cur, conn)
    make_weather_table("weather_data.json", cur, conn)
    make_health_table("health_data.json", cur, conn)
    make_elevation_table("elevation_data.json", cur, conn)
    make_sun_table("sun_data.json", cur, conn)
    # sun_depression(cur, "sun_depression.csv")
    # join_tables(cur, conn)
    # calculations_one(cur,conn)
    # calculations_two(cur,conn)
    # make_plot_one(cur,conn)
    # make_plot_two(cur,conn)

if __name__ == "__main__":
    main()