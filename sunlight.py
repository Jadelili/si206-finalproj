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


def cache_sun_data(three_city_d, sunfile):
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
    write_json(sunfile, sun_d)


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
            
            d["temp"] = temp / day_num
            d["app_temp"] = app_temp / day_num
            d["rad"] = rad / day_num
            d["prec"] = prec / day_num
            d["rain"] = rain / day_num
            d["snow"] = snow / day_num
            d["prec_hours"] = prec_h / day_num
            d["sunlight_hours"] = duration / day_num

            if state == "WV" and city == "Charleston":
                city_d["Charleston_WV"] = d
            elif city not in city_d.keys():
                city_d[city] = d
            elif state == "ME" and city == "Portland":
                city_d["Portland"] = d
            elif state == "WV" and city == "Charleston":
                city_d["Charleston"]= d
            elif state == "MD" and city == "Columbia":
                city_d["Columbia"] = d
        
        if state not in list(sun_d.keys()):
            sun_d[state] = city_d
        else:
            sun_d[state].update(city_d)
    write_json(sunfile, sun_d)
    return sun_d




def main():
    three_city_d = get_lat("health_data_r.json", "coordinate.json")
    ### cache_sun_data(three_city_d, "sun_data_r.json")
    process_sun_data("sun_data_r.json", "sun_data.json")
    # process_health_data("health_data_r.json", "health_data.json", three_city_d)
    # cur, conn = open_database("Mental_health.db")
    # make_state_table("weather_data.json", cur, conn)
    # make_weather_table("weather_data.json", cur, conn)
    # make_health_table("health_data.json", cur, conn)
    # make_elevation_table("elevation_data.json", cur, conn)
    # make_state_city_table("location_data.json", cur, conn)

if __name__ == "__main__":
    main()
