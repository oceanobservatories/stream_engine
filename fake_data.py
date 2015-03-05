import uuid
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('ooi2')

ctdbp_no_sample = {
    'subsite': 'TEST',
    'node': 'TEST',
    'sensor': 'TEST',
    'method': 'streamed',
    'conductivity': 1396258,
    'oxy_calphase': 34407,
    'oxy_temp': 23002,
    'oxygen': 1724210,
    'pressure': 8597725,
    'pressure_temp': 28282,
    'provenance': 0,
    'quality_flag': 'ok',
    'temperature': 418687,
    'preferred_timestamp': 'port_timestamp'
}

ps = session.prepare('insert into ctdbp_no_sample (subsite,node,sensor,method,time,id,conductivity,driver_timestamp,'
                     'ingestion_timestamp,internal_timestamp,oxy_calphase,oxy_temp,oxygen,port_timestamp,'
                     'preferred_timestamp,pressure,pressure_temp,provenance,quality_flag,temperature) values'
                     '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')

for t in xrange(1, 200000):
    ctdbp_no_sample['time'] = float(t)
    ctdbp_no_sample['internal_timestamp'] = float(t)
    ctdbp_no_sample['driver_timestamp'] = float(t)
    ctdbp_no_sample['ingestion_timestamp'] = float(t)
    ctdbp_no_sample['port_timestamp'] = float(t)
    ctdbp_no_sample['id'] = uuid.uuid4()
    session.execute(ps, ctdbp_no_sample)