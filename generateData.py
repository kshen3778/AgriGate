import h5py
import numpy as np
import psycopg2
import sys

conn = psycopg2.connect(database='agrigate',
                        user='maxroach', host='localhost', port=26257)
conn.set_session(autocommit=True)
cur = conn.cursor()


def updatedb(dataType, data, latitude, longitude):
    length = len(data)
    for i in range(length):
        if i % 100 == 0:
            print(i, '/', length, (i / length) * 100, '%')
        dataList = []
        for lat, lng, dat in zip(latitude[i], longitude[i], data[i]):
            if not (dat == -9999.0 and dataType == 'moisture') and not (dat in [0.0, 254.0, 255.0] and dataType == 'landcover'):
                dataList.append(cur.mogrify(
                    '(%s,%s,%s)', [lat, lng, dat]).decode('utf-8'))
        if dataList:
            dataText = ','.join(dataList)
            cur.execute(
                'INSERT INTO ' + cur.mogrify('%s', dataType).decode('utf-8') + ' (lat, lng, val) VALUES ' + dataText)

f = h5py.File('data/SMAP_L3_SM_P_20160915_R13080_001.h5', 'r')
# Latitude + Longitude
latitude = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'latitude']).tolist()
longitude = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'longitude']).tolist()
# Moisture
moisture = np.array(f[u'Soil_Moisture_Retrieval_Data']
                    [u'soil_moisture']).tolist()
# Landcover
landcover = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'landcover_class'])
# Surface Flag
surface_flag = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'surface_flag'])
f.close()
for i in sys.argv:
    if i == 'm':
        updatedb('moisture', moisture, latitude, longitude)
    elif i == 'l':
        updatedb('landcover', landcover, latitude, longitude)
    elif i == 's':
        updatedb('surface_flag', surface_flag, latitude, longitude)

cur.close()
conn.close()
