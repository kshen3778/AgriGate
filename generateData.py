import h5py
import numpy as np
import psycopg2

conn = psycopg2.connect(database='agrigate', user='maxroach', host='localhost', port=26257)
conn.set_session(autocommit=True)
cur = conn.cursor()

def updatedb(moisture,latitude,longitude):
    for i in range(len(moisture)):
        dataList = []
        for lat, lon, moi in zip(latitude[i], longitude[i], moisture[i]):
            if moi != -9999.0:
                dataList.append(cur.mogrify('(%s,%s,%s)', [lat,lon,moi]).decode('utf-8'))
        if dataList:
            dataText = ','.join(dataList)
            cur.execute('INSERT INTO moisture (lat, lon, val) Values ' + dataText)

f = h5py.File('data/SMAP_L3_SM_P_20160915_R13080_001.h5', 'r')
moisture = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'soil_moisture']).tolist()
latitude = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'latitude']).tolist()
longitude = np.array(f[u'Soil_Moisture_Retrieval_Data'][u'longitude']).tolist()
f.close()
updatedb(moisture, latitude, longitude)

cur.close()
conn.close()
