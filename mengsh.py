#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import argparse
import pymongo
import mongoengine as meng
from pp import pp

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument('--host', default = "mongodb://127.0.0.1/default")    
parser.add_argument('--host1')
parser.add_argument('--host2')
parser.add_argument('--host3')    

args = parser.parse_args()

HOSTNAMES = ['host', 'host1', 'host2', 'host3']

hosts = []
dbs = []
for hostname in HOSTNAMES:
    mongo = getattr(args, hostname)
    if not mongo:
        continue
    if mongo.find("mongodb://") != 0:
        mongo = "mongodb://127.0.0.1:27017/" + mongo
#     print mongo
    con = meng.connect(host = mongo, alias=hostname)
    hosts.append(con)
    dbs.append(con.get_default_database())
    globals()[hostname] = con
    globals()[hostname.replace('host','db')] = con.get_default_database()

create_template="""
class {0}(meng.DynamicDocument):
    _id = meng.DynamicField(primary_key = True)
    def __repr__(self):
        return pp(self, as_str=True)
    meta = {{
        'collection': '{1}',
        'db_alias': '{2}'
    }}
globals()['{0}'] = {0}
"""

def create(*args, **kw):
    classname = list(kw.keys())[0]
    collectionname = classname
    host = kw[classname]
    hostname = 'host'
    if host > 0:
        classname += "_%s" % host
        hostname += "%s" % host
    exe = create_template.format(classname, collectionname, hostname)
#     print exe                        
    exec(exe)
    
def refresh():
    for hostname in HOSTNAMES:
        mongo = getattr(args, hostname)
        if not mongo:
            continue
        i = 0 if hostname=='host' else int(hostname.replace('host', ''))
        d = globals()[hostname.replace('host', 'db')]
        print(i, d)
        for c in d.collection_names():
            if c != "system.indexes":
                print("host %s collection %s" % (i, c))
                create(**{c:i})
# 
# create(foo = 0)
# create(foo = 2)
# print foo.objects
# print foo_2.objects
refresh()