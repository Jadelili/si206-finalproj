import matplotlib.pyplot as plt
from weather_health_load import open_database
import pandas as pd
import numpy as np


def coe(cur, conn, state_list):
    cur.execute('''SELECT Sun.city_id, Sun.city_name, Sun.state_id, State.state_abbr, 
    Sun.sunlight_hours, Sun.rad,
    Health.depression
    FROM Sun 
    JOIN Health ON Sun.city_id = Health.city_id
    JOIN State ON Sun.state_id = State.id''')
    sun_dep = cur.fetchall()

    state_d = {}
    for state in state_list:
        d = {}
        sunlight_hours = 0
        radiation = 0
        depression = 0
        # mh_not_good = 0
        count = 0
        for tup in sun_dep:
            if tup[-1] != "null":
                if tup[3] == state:
                    sunlight_hours += float(tup[4])
                    radiation += float(tup[5])
                    depression += float(tup[-1])
                    # mh_not_good += float(tup[6])
                    count += 1

                    d["sunlight_hours"] = round(sunlight_hours / count, 2)
                    d["radiation"] = round(radiation / count, 2)
                    d["depression"] = round(depression / count, 2)
                    # d["mh_not_good"] = round(mh_not_good / count, 2)
                    state_d[tup[3]] = d

    dep_list = []
    hour_list = []
    rad_list = []
    mh_not_good_list = []
    for state in state_d:
        dep_list.append(state_d[state]["depression"])
        hour_list.append(state_d[state]["sunlight_hours"])
        rad_list.append(state_d[state]["radiation"])

    fig = plt.figure(figsize=(10,4))
    fig.tight_layout()
    ax1 = fig.add_subplot(121)
    ax1.set_title("Depression vs Sunlight Duration of the Day", fontsize=12)
    ax1.set_xlabel("Sunlight Duration")
    ax1.set_ylabel("Depression")
    ax1.grid()

    for state in state_d:
        plt.scatter(state_d[state]["sunlight_hours"], state_d[state]["depression"], c="lightblue")
    ax1.set_xlim(580,680)
    ax1.set_ylim(12,30)
    
    
    ax2 = fig.add_subplot(122)
    ax2.set_title("Depression vs Radiation", fontsize=12)
    ax2.set_xlabel("Radiation")
    ax2.set_ylabel("Depression")
    ax2.grid()
    
    for state in state_d: 
        plt.scatter(state_d[state]["radiation"], state_d[state]["depression"], c="lightgreen")
    ax2.set_xlim(0,20)
    ax2.set_ylim(12,30)
    plt.show()



def main():
    cur, conn = open_database("Mental_health.db")
    cur.execute('''SELECT state_abbr from State''')
    result = cur.fetchall()
    state_list = []
    for i in result:
        state_list.append(i[0])

    coe(cur, conn, state_list)


    
if __name__ == "__main__":
    main()



