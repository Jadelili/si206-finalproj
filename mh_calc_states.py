import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from weather_health_load import open_database


def create_radar_chart():
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
    for col in radar_cols:
        df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    angles = np.linspace(0, 2 * np.pi, len(radar_cols), endpoint=False)
    values = []
    for i, state in enumerate(df.index):
        value = df.loc[state, radar_cols].values.flatten().tolist()
        value += [value[0]]  # Add the first value to the end to close the circle
        scaled_value = [val * (len(radar_cols) / (len(radar_cols) + 1)) for val in value[:-1]]  # Slice to exclude last value
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
    plt.show()


if __name__ == '__main__':
    create_radar_chart()
