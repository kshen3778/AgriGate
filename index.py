import psycopg2
import hug

conn = psycopg2.connect(database='agrigate', user='maxroach', host='localhost', port=26257)
conn.set_session(autocommit=True)
cur = conn.cursor()

@hug.get('/moisture', examples='c1=-38.859642028808594,-67.03319549560547&c2=-44.10889434814453,170.85061645507812')
def moisture(c1, c2):
    """Returns moisture levels in soil in between 2 given coordinates"""
    lat1, lon1 = map(float, c1)
    lat2, lon2 = map(float, c2)
    cur.execute('SELECT * FROM moisture WHERE lat BETWEEN %s AND %s AND lon BETWEEN %s AND %s', [min(lat1,lat2),max(lat1,lat2),min(lon1,lon2),max(lon1,lon2)])
    moisture = cur.fetchall()
    return {'moisture': moisture}
