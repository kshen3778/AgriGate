import h5py
import shapefile
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
        # HACK
        for lat, lng, dat in zip(latitude[i], longitude[i], data[i]):
            if not dat == -9999.0:
                dataList.append(cur.mogrify(
                    '(%s,%s,%s)', [lat, lng, dat]).decode('utf-8'))
        if dataList:
            dataText = ','.join(dataList)
            cur.execute(
                'INSERT INTO ' + dataType + ' (lat, lng, val) VALUES ' + dataText)

f = h5py.File('data/SMAP_L4_SM_gph_20160914T223000_Vv2030_001.h5', 'r')
sf = shapefile.Reader('data/ca_all_slc_v3r2.shp')
# Shapefile
shapes = sf.shapes()
shapeRecs = sf.shapeRecords()
shapeRec = sf.shapeRecord(3)
points = shapeRecs[3].shape.points[0:2]
# Latitude + Longitude
latitude = np.array(f[u'cell_lat']).tolist()
longitude = np.array(f[u'cell_lon']).tolist()
# Moisture
moisture = np.array(f[u'Geophysical_Data']
                    [u'sm_rootzone']).tolist()
# Landcover
vegetation = np.array(f[u'Geophysical_Data']
                      [u'vegetation_greenness_fraction']).tolist()
f.close()
for i in sys.argv:
    if i == 'm':
        updatedb('moisturel4', moisture, latitude, longitude)
    elif i == 'v':
        updatedb('vegetationl4', vegetation, latitude, longitude)
    elif i == 's':
        for row in shapeRecs:
            allLat = 0
            allLon = 0
            curRow = row.shape.points
            for latlon in curRow:
                allLat += latlon[1]
                allLon += latlon[0]
            allLat = allLat / len(curRow)
            allLon = allLon / len(curRow)
            if not row.record[2] == '-':
                cur.execute('INSERT INTO loose_soil (lat, lng, val) VALUES (%s, %s, %s)', [
                            allLat, allLon, row.record[2]])

cur.close()
conn.close()
