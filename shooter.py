import ulmo
import pandas
import numpy as np

from multiprocessing import Pool

import warnings


def get_daily_normals(station_id):
    print('Receiving data for station {}'.format(station_id))
    warnings.filterwarnings('ignore')
    data = ulmo.ncdc.ghcn_daily.get_data(station_id, as_dataframe=True)
    print('Data for station {} received'.format(station_id))

    if 'TMAX' not in data.keys():
        print('TMAX not in {}, returning empty DataFrame'.format(station_id))
        return pandas.DataFrame()

    tm = data['TMAX'].copy()
    tm.value = tm.value.astype(float)
    tm = tm[np.isfinite(tm.value)]
    tm.value = tm.value / 10
    tm.index.name = 'date'
    tm.reset_index(inplace=True)
    tm['Day'] = tm.date.astype(str).str.slice(5)
    return tm.groupby(['Day']).mean()


def add_station_info(daily_normals, station):
    daily_normals['country'] = station.country
    daily_normals['name'] = station.name
    daily_normals['latitude'] = station.latitude
    daily_normals['longitude'] = station.longitude
    daily_normals['id'] = station.id


def process_station(current_station):
    print('Begin processing station {}'.format(current_station.id))
    dn = get_daily_normals(current_station.id)
    if len(dn) != 366:
        print('Station {} is not full'.format(current_station.id))
        current_station.Full = False

    else:
        add_station_info(dn, current_station)
        dn.to_csv('data/{}.csv'.format(current_station.id))
        print('Finished processing station {}'.format(current_station.id))


def update_stations(stations):
    stations = stations[~stations.Complete]
    stations = stations[stations.Full]


if __name__ == "__main__":
    stations = pandas.read_csv('stations.csv')
    print('Received station list')
    update_stations(stations)
    print('Updated stations; {} stations left...'.format(len(stations)))

    station_list = list()

    for i, r in stations.iterrows():
        station_list.append(r)

    p = Pool(8)

    p.map(process_station,station_list)

    stations_new = pandas.concat(station_list, axis=1).T

    stations_new.to_csv('stations.csv')