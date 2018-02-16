#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import argparse, time
import pymongo
import mongoengine as meng
from pp import pp
from IPython.utils.py3compat import builtin_mod_name
from pydoc import classname

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument('--host')    
parser.add_argument('--host1')
parser.add_argument('--host2')
parser.add_argument('--host3')    

args = parser.parse_args()

if not (args.host or args.host1 or args.host2 or args.host3):
    print ("\x1b[1;35musage: ipython3 -i -- --host <host>")
    print ("or:    ipython3 -i -- --host <firsthost> [--host2 <secondhost> [--host3 <thirdhost>]]")
    print ("where host is a database name (default localhost:27017) or a fully-qualified mongodb path")
    print ("such as mongodb://[user:password]@ipaddr_or_url/database\x1b[0m")
    exit()

PURPLE = '\x1b[1;35m%s\x1b[0m'

HOSTNAMES = ['host', 'host1', 'host2', 'host3']

hosts = []
dbs = []
def init():
    for hostname in HOSTNAMES:
        mongo = getattr(args, hostname)
        if not mongo:
            continue
        if mongo.find("mongodb://") != 0:
            mongo = "mongodb://127.0.0.1:27017/" + mongo
    #     print mongo
        con = meng.connect(host = mongo, alias=hostname)
        con.mengsh_alias = hostname
        hosts.append(con)
        db = con.get_default_database()
        db.mengsh_alias = hostname.replace('host','db')
        dbs.append(db)
        print ("DBG", db.mengsh_alias, db)
        globals()[hostname] = con
        globals()[hostname.replace('host','db')] = db

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
    classname = classname.replace('.','_')
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
            if c.find("system.") != 0:
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

def get_host_tag(h):
    try:
        return int(h.mengsh_alias[-1])
    except:
        return 0

def get_stats(col):
    name, ignor = get_base_tag(col.__name__)
    return col._collection.database.command("collstats", name)
    
def copy(source,        #must be a collection 
         dest,          #db, string, or collection
         **kw):         #query filter on source to copy
    sname, ignore = get_base_tag(source.__name__)
    if type(dest) == str:
        dname, dtag = get_base_tag(dest)
    else:
        if dest.__class__.__name__ == "Database":
            dname = sname
            dtag = get_host_tag(dest)
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
    scnt = q.count()
    dcnt = dest.objects.count()
    print ("DBG", source, dest)
    i = input("copying %d documents from %s:%s/%s/%s to %s:%s/%s/%s(%d already)\n%selete, %sverwrite, %serge, %sest, %sbort?" %
             (scnt, 
              source._collection.database.client.address[0], source._collection.database.client.address[1], source._collection.database.name, source.__name__,
              dest._collection.database.client.address[0], dest._collection.database.client.address[1], dest._collection.database.name, dest.__name__,
              dcnt,
              PURPLE % 'd',
              PURPLE % 'o',
              PURPLE % 'm',
              PURPLE % 't',
              PURPLE % 'a',
              ))
    over = False
    real = True
    if i == 'd':
        print ("dropping %s" % dest)
        dest._collection.drop()
    elif i == 'o':
        print ("overwriting existing data")
        over = True
    elif i == 'm':
        print ("merging data; id conflicts will cause exception")
    elif i == 't':
        print ("test dry run, not saving to destination")
        real = False
    else:
        print ("aborting")
        return
    avgsize = get_stats(source)['avgObjSize']
    bytes = 0
    print ("average document size:", avgsize)
    n = 0
    t0 = time.time()
    every = max(min(500, scnt//10), 1)
    for x in q:
#         time.sleep(.001)
#         print (" copying", x._id)
        xm = x.to_mongo()
        if real:
            if over:
                dest._collection.replace_one({'_id': xm['_id']}, xm, upsert = True)
            else:
                try:
                    dest._collection.insert_one(xm)
                except:
                    print ("failed to copy %s                           " % xm['_id'])
        n += 1
        bytes += avgsize
        if n % every == 0 or n == scnt:
            dt = time.time() - t0
            if dt:
                est = scnt/n * dt - dt
                bpers = bytes/dt
            else:
                est = 999999
                bpers = 0
            hr = est // 3600
            mn = (est // 60) % 60
            sec = est % 60
            print ("%d of %d copied, %.3f%% done, time remaining: %d:%02d:%05.2f mbytes/sec: %.3f" %
                    (n, scnt, n*100/scnt, hr, mn, sec, bpers/1000000), end="   \r")

init()
refresh()
