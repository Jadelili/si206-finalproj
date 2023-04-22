from weather_health_load import open_database
from weather_health_load import load_json
from weather_health_load import write_json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd

def calculate_health_data(lst, filename):
    scaled_x = (lst - np.min(lst)) / (np.max(lst) - np.min(lst)) * 10
    f = open(filename)
    return scaled_x


def write_overview(cur, conn, state_list, filename):
    cur.execute('''SELECT Health.city_id, Health.city_name, Health.state_id, State.state_abbr, 
    Health.depression, Health.mh_not_good, Health.sleep_less_7, Health.no_leis_phy_act
    FROM Health 
    JOIN State ON Health.state_id = State.id''')
    sun_dep = cur.fetchall()

    dep_list = []
    mh_list = []
    sleep_list = []
    leisure_list = []
    for state in state_list:
        depression = 0
        mh_not_good = 0
        sleep_less_than_7 = 0
        no_leissure_phy = 0
        count = 0
        for tup in sun_dep:
            if tup[-1] != "null":
                if tup[3] == state:
                    depression += float(tup[4])
                    mh_not_good += float(tup[5])
                    sleep_less_than_7 += float(tup[6])
                    no_leissure_phy += float(tup[7])
                    count += 1
                    dep_avg = round(depression / count, 2)
                    mh_avg = round(mh_not_good / count, 2)
                    sleep_avg = round(sleep_less_than_7 / count, 2)
                    act_avg = round(no_leissure_phy / count, 2)
        dep_list.append(dep_avg)
        mh_list.append(mh_avg)
        sleep_list.append(sleep_avg)
        leisure_list.append(act_avg)

    s_dep_list = scaled_x = (dep_list - np.min(dep_list)) / (np.max(dep_list) - np.min(dep_list)) * 10
    s_mh_list = (mh_list - np.min(mh_list)) / (np.max(mh_list) - np.min(mh_list)) * 10
    s_sleep_list = (sleep_list - np.min(sleep_list)) / (np.max(sleep_list) - np.min(sleep_list)) * 10
    s_leisure_list = (leisure_list - np.min(leisure_list)) / (np.max(leisure_list) - np.min(leisure_list)) * 10

    state_d = {}
    for i in range(len(state_list)):
        d = {}
        d["depression_avg"] = round(s_dep_list[i], 2)
        d["mental_health_not_good_avg"] = round(s_mh_list[i], 2)
        d["sleep_less_than_7_avg"] = round(s_sleep_list[i], 2)
        d["no_leisure_physical_activity_avg"] = round(s_leisure_list[i], 2)
        state_d[state_list[i]] = d
    write_json(filename, state_d)


def vis_overview(state_list, filename):
    state_d = load_json(filename)
    dep_list = []
    mh_list = []
    sleep_list = []
    leisure_list = []
    for i in state_list:
        dep_list.append(state_d[i]["depression_avg"])
        mh_list.append(state_d[i]["mental_health_not_good_avg"])
        sleep_list.append(state_d[i]["sleep_less_than_7_avg"])
        leisure_list.append(state_d[i]["no_leisure_physical_activity_avg"])

    data_dict = {'State': list(state_d.keys()), 'Depression Rate': dep_list, 'Mental Health Not Good': mh_list,
                'Sleep Less Than 7 Hours': sleep_list, 'No Leisure Physical Exercise': leisure_list,}
    
    df = pd.DataFrame(data_dict)
    df.set_index('State', inplace=True)
    sns.set()
    fig, ax = plt.subplots(figsize=(8, 16))
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    sns.heatmap(df, cmap='YlGnBu', annot=True, linewidths=.3, ax=ax)
    ax.tick_params(axis='both', labelsize=8)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=10, horizontalalignment='right', fontsize=8)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, horizontalalignment='right', fontsize=8)

    ax.set_title('Heatmap of US State Health Data', fontsize=10)
    ax.set_xlabel('Category', fontsize=8)
    ax.set_ylabel('State', fontsize=8)
    plt.savefig('dep_states_overview')


def write_dep_sun_states(cur, conn, state_list, filename):
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
        count = 0
        for tup in sun_dep:
            if tup[-1] != "null":
                if tup[3] == state:
                    sunlight_hours += float(tup[4])
                    radiation += float(tup[5])
                    depression += float(tup[-1])
                    count += 1

                    d["sunlight_hours"] = round(sunlight_hours / count, 2)
                    d["radiation"] = round(radiation / count, 2)
                    d["depression"] = round(depression / count, 2)
                    state_d[tup[3]] = d
    write_json(filename, state_d)



def vis_dep_sun_states(state_list, filename):
    state_d = load_json(filename)
    dep_list = []
    hour_list = []
    rad_list = []
    for state in state_d:
        dep_list.append(state_d[state]["depression"])
        hour_list.append(state_d[state]["sunlight_hours"])
        rad_list.append(state_d[state]["radiation"])

    fig = plt.figure(figsize=(10,4))
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
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
    plt.savefig('dep_sun_corr')

def create_radar_chart(cur, conn):
        cur, conn = open_database('Mental_health.db')
        radar_cols = ['mh_not_good', 'depression', 'no_leis_phy_act', 'sleep_less_7']
        cur.execute('''SELECT State.state_abbr, 
                            AVG(Health.mh_not_good) AS mh_not_good, 
                            AVG(Health.depression) AS depression, 
                            AVG(Health.no_leis_phy_act) AS no_leis_phy_act, 
                            AVG(Health.sleep_less_7) AS sleep_less_7
                    FROM Health
                    JOIN State ON Health.state_id = State.id
                    GROUP BY State.state_abbr''')
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=['state'] + radar_cols)
        df.set_index('state', inplace=True)
        df_orig = df.copy()
        for col in radar_cols:
            df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
        for col in radar_cols:
            df[col+'_orig'] = df_orig[col]
        angles = np.linspace(0, 2 * np.pi, len(radar_cols), endpoint=False)
        values = []
        for i, state in enumerate(df.index):
            value = df.loc[state, radar_cols].values.flatten().tolist()
            value += [value[0]]  
            scaled_value = [val * (len(radar_cols) / (len(radar_cols) + 1)) for val in value[:-1]] 
            values.append(scaled_value)
        fig = plt.figure(figsize=(15, 10))
        ax = fig.add_subplot(111, polar=True)
        ax.set_theta_offset(np.pi / 1.5)
        ax.set_theta_direction(-1)
        ax.set_rlabel_position(0)
        for i, state in enumerate(df.index):
            ax.plot(angles, values[i], linewidth=1, linestyle='solid', label=state)
            ax.fill(angles, values[i], alpha=0.1)
        ax.set_thetagrids(angles * 180 / np.pi, radar_cols)
        legend = ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0., ncol=2)
        ax.set_title('Health Indicators by State', fontsize=16)
        with open('radar_chart_values.txt', 'w') as f:
            f.write('Unscaled values:\n')
            f.write(df_orig.to_string(float_format='%.3f', columns=radar_cols))
            f.write('\n\nScaled values:\n')
            f.write(df.to_string(float_format='%.3f', columns=radar_cols))
        
        fig.savefig('radar_chart.png')
        plt.close(fig)


def main():
    cur, conn = open_database("Mental_health.db")
    cur.execute('''SELECT state_abbr from State''')
    result = cur.fetchall()
    state_list = []
    for i in result:
        state_list.append(i[0])
    write_overview(cur, conn, state_list, "CALC_dep_states_overview.json")
    vis_overview(state_list, "CALC_dep_states_overview.json")
    write_dep_sun_states(cur, conn, state_list, "CALC_dep_sun_corr.json")
    vis_dep_sun_states(state_list, "CALC_dep_sun_corr.json")
    create_radar_chart(cur, conn)


    
if __name__ == "__main__":
    main()



