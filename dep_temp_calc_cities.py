import matplotlib.pyplot as plt
from weather_health_load import open_database
import pandas as pd
import numpy as np


def coe(cur, conn, city_list):
    cur.execute('''SELECT Weather.id, Weather.city_name, Weather.state_id, State.state_abbr, 
    Weather.temp,
    Health.depression
    FROM Weather 
    JOIN Health ON Weather.id = Health.city_id
    JOIN State ON Weather.state_id = State.id''')
    weather_dep = cur.fetchall()

    city_d = {}
    for city in city_list:
        d = {}
        temp = 0
        depression = 0
        count = 0
        for tup in weather_dep:
            if tup[-1] != "null":
                if tup[1] == city:
                    temp += float(tup[4])
                    depression += float(tup[-1])
                    count += 1

                    d["temp"] = round(temp / count, 2)
                    d["depression"] = round(depression / count, 2)
                    city_d[tup[1]] = d

    dep_list = []
    temp_list = []
    for city in city_d:
        dep_list.append(city_d[city]["depression"])
        temp_list.append(city_d[city]["temp"])

    fig = plt.figure(figsize=(6,4))
    fig.tight_layout()
    ax = fig.add_subplot(111)
    ax.set_title("Depression vs Temperature", fontsize=12)
    ax.set_xlabel("Temperature (F)")
    ax.set_ylabel("Depression")
    ax.grid()

    for city in city_d:
        plt.scatter(city_d[city]["temp"], city_d[city]["depression"], c="lightblue")
    ax.set_xlim(20,90)
    ax.set_ylim(12,30)
    plt.show()


def main():
    cur, conn = open_database("Mental_health.db")
    cur.execute('''SELECT city_name from Health''')
    result = cur.fetchall()
    state_list = []
    for i in result:
        state_list.append(i[0])

    coe(cur, conn, state_list)


if __name__ == "__main__":
    main()
