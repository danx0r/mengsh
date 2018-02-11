#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import argparse, time
import pymongo
import mongoengine as meng
from pp import pp
from IPython.utils.py3compat import builtin_mod_name

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
    return globals()[classname]
    
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
                col = create(**{c:i})
                print("host %s %6d documents in %s" % (i, col.objects.count(), col.__name__))

def get_base_tag(s):
    if s[-2:-1] == '_':
        base = s[:-2]
        tag = int(s[-1])
    else:
        base = s
        tag = 0
    return base, tag

def tag_to_string(t):
    return ('_%s' % t) if t else ''

def copy(source,        #must be a collection 
         dest,          #db, string, or collection
         **kw):         #query filter on source to copy
    sname, ignore = get_base_tag(source.__name__)
    if type(dest) == str:
        dname, dtag = get_base_tag(dest)
    else:
        if dest.__class__.__name__ == "Database":
            dname = sname
            dtag = dbs.index(dest)
        else:
            dname, dtag = get_base_tag(dest.__name__)
    dfullname = dname + tag_to_string(dtag)
    if dfullname == source.__name__:
        print ("cannot copy %s to itself" % dfullname)
        return
    try:
        dest = globals()[dfullname]
    except:
        print ("creating dest:", dname + tag_to_string(dtag))
        create(**{dname:dtag})
    dest = globals()[dname + tag_to_string(dtag)]
#     print ("dest:", dest)
    q = source.objects(**kw)
#     print (type(q), q.count())
    scnt = source.objects.count()
    dcnt = dest.objects.count()
    i = input("copying %d documents from %s to %s(%d already) -- d(elete), m(erge), a(bort)?" %
             (scnt, source.__name__, dest.__name__, dcnt))
    if i == 'd':
        print ("dropping %s" % dest)
        dest._collection.drop()
    elif i != 'm':
        print ("aborting")
        return
    n = 0
    t0 = time.time()
    every = max(min(500, scnt//10), 1)
    for x in q:
#         print (" copying", x._id)
        dest._collection.save(x.to_mongo())
        n += 1
        if n % every == 0:
            dt = time.time() - t0
            if dt:
                est = scnt/n * dt - dt
            else:
                est = 999999
            hr = est // 3600
            mn = (est // 60) % 60
            sec = est % 60 
            print ("%d of %d copied, %.3f%% done, time remaining: %d:%02d:%05.2f" % (n, scnt, n*100/scnt, hr, mn, sec), end="   \r")
refresh()
