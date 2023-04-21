import matplotlib.pyplot as plt
from weather_health_load import open_database

def coe(cur, conn):
    cur.execute('''SELECT Sun.city_id, Sun.city_name, Health.no_leis_phy_act, Health.depression
                   FROM Sun 
                   JOIN Health ON Sun.city_id = Health.city_id''')
    city_dep = cur.fetchall()

    city_d = {}
    for tup in city_dep:
        if tup[-1] != "null":
            if tup[1] not in city_d:
                city_d[tup[1]] = {"no_leis_phy_act": 0, "depression": 0, "count": 0}
            city_d[tup[1]]["no_leis_phy_act"] += float(tup[2])
            city_d[tup[1]]["depression"] += float(tup[-1])
            city_d[tup[1]]["count"] += 1

    dep_list = []
    no_leis_phy_act_list = []
    for city in city_d:
        if city_d[city]["count"] > 0:
            dep_list.append(city_d[city]["depression"] / city_d[city]["count"])
            no_leis_phy_act_list.append(city_d[city]["no_leis_phy_act"] / city_d[city]["count"])

    fig = plt.figure(figsize=(6,6))
    fig.tight_layout()
    ax1 = fig.add_subplot(111)
    ax1.set_title("Depression vs No Leisure Time Physical Activity", fontsize=12)
    ax1.set_xlabel("No Leisure Time Physical Activity")
    ax1.set_ylabel("Depression")
    ax1.grid()

    for city in city_d:
        if city_d[city]["count"] > 0:
            plt.scatter(city_d[city]["no_leis_phy_act"] / city_d[city]["count"], city_d[city]["depression"] / city_d[city]["count"], c="lightblue")

    plt.show()

def main():
    cur, conn = open_database("Mental_health.db")
    coe(cur, conn)

if __name__ == "__main__":
    main()
