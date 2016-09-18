import psycopg2
import hug

conn = psycopg2.connect(database='agrigate',
                        user='maxroach', host='localhost', port=26257)
conn.set_session(autocommit=True)
cur = conn.cursor()


def cors_support(response, *args, **kwargs):
    response.set_header('Access-Control-Allow-Origin', '*')


@hug.get('/moisture', requires=cors_support, examples='c1=-38.859642028808594,-67.03319549560547&c2=-44.10889434814453,170.85061645507812')
def moisture(c1, c2):
    """Returns moisture levels in soil between 2 given coordinates"""
    lat1, lng1 = map(float, c1)
    lat2, lng2 = map(float, c2)
    cur.execute('SELECT * FROM moisturel4 WHERE lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s',
                [min(lat1, lat2), max(lat1, lat2), min(lng1, lng2), max(lng1, lng2)])
    moisture = cur.fetchall()
    return {'moisture': moisture}


@hug.get('/vegetation', requires=cors_support, examples='c1=-38.859642028808594,-67.03319549560547&c2=-44.10889434814453,170.85061645507812')
def vegetation(c1, c2):
    """Returns vegetation density between 2 given coordinates"""
    lat1, lng1 = map(float, c1)
    lat2, lng2 = map(float, c2)
    cur.execute('SELECT * FROM vegetationl4 WHERE lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s',
                [min(lat1, lat2), max(lat1, lat2), min(lng1, lng2), max(lng1, lng2)])
    vegetation = cur.fetchall()
    return {'vegetation': vegetation}


@hug.get('/allMoisture', requires=cors_support)
def allMoisture():
    """Returns all moisture levels in soil"""
    cur.execute('SELECT * FROM moisturel4')
    moisture = cur.fetchall()
    return {'moisture': moisture}


@hug.get('/looseSoil', requires=cors_support, examples='c1=-38.859642028808594,-67.03319549560547&c2=-44.10889434814453,170.85061645507812')
def soilloose(c1, c2):
    """Returns how loose the soil is"""
    lat1, lng1 = map(float, c1)
    lat2, lng2 = map(float, c2)
    cur.execute('SELECT * FROM loose_soil WHERE lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s',
                [min(lat1, lat2), max(lat1, lat2), min(lng1, lng2), max(lng1, lng2)])
    loose = cur.fetchall()
    return {'loose': loose}
