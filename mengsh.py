#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import argparse, sys, time
import psutil
import mongoengine as meng
import pymongo
import bson
from pprint import pprint as pp
from pprint import pformat

def _prompt_drop_collection(*args, **kw):
    print ("drop", args, kw)
    if input("are you sure? (type 'yes' to proceed):")[:3] == 'yes':
        print ("ok, here goes")
        _real_drop_collection(*args, **kw)
    else:
        print ("drop_collection cancelled")

_real_drop_collection = pymongo.database.Database.drop_collection
pymongo.database.Database.drop_collection = _prompt_drop_collection

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
        print (mongo)
        con = meng.connect(host = mongo, alias=hostname, socketTimeoutMS=300000)
        print (con)
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
        return pformat(self.to_mongo().to_dict())
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
    classname = classname.replace('-','_')
    hostname = 'host'
    if host > 0:
        classname += "_%s" % host
        hostname += "%s" % host
    exe = create_template.format(classname, collectionname, hostname)
#     print exe
    try:
        exec(exe)
    except:
        print ("failed:", collectionname)
        return
    return globals()[classname]
    
def refresh():
    for hostname in HOSTNAMES:
        totbytes = totcol = 0
        mongo = getattr(args, hostname)
        if not mongo:
            continue
        i = 0 if hostname=='host' else int(hostname.replace('host', ''))
        d = globals()[hostname.replace('host', 'db')]
        print(i, d)
        colnames = d.collection_names()
        colnames.sort()
        for c in colnames:
            if c.find("system.") != 0:
                col = create(**{c:i})
                if col == None:
                    continue
                col.objects.limit(1)    #access forces _collection.database to exist
                colcnt = col.objects.count()
                totcol += colcnt
                try:
                    info = get_stats(col)
                    print("host {} {:>10,} docs {:>9,} avg {:>16,} tot   {}". 
                      format(i, colcnt, (info['avgObjSize'] if info['size'] else 0), int(info['size']), col.__name__))
                    totbytes += info['size']
                except:
                    print ("    %s collection not found" % col)
        print ("{}: {:,} bytes total in {} collections, {:,} documents".format(hostname, int(totbytes), len(colnames), totcol))

def get_base_tag(s):
    if s[-2:-1] == '_':
        try:
            tag = int(s[-1])
            base = s[:-2]
        except:
            base = s
            tag = 0
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
    # name, ignor = get_base_tag(col.__name__)
    name = col._collection._Collection__name
    return col._collection.database.command("collstats", name)

def collections(db, show=False):
    tag = tag_to_string(get_host_tag(db))
    cols = []
    for x in db.collection_names():
        x += tag
        try:
            c = globals()[x]
            size = get_stats(c)['size']
            cols.append((size, c))
        except:
            print ("cannot find collection %s" % x)
    cols.sort(key=lambda x:x[0])
    if show:
        for c in cols:
            print ("{:17,} {}".format(int(c[0]), c[1].__name__))
    else:
        cols = [x[1] for x in cols]
        return cols

def count(col, field):
    col.objects.limit(1)    #access forces _collection to exist
    values = col._collection.distinct(field)
    total = col._collection.count()
    some = 0
    ret = []
    for v in values:
        if v != None:
            cnt = col.objects(**{field: v}).count()
            some += cnt
            ret.append((v, cnt))
    ret.append((None, total-some))
    ret.sort(key=lambda x:x[1], reverse=True)
    return ret

def get_indices(col):
    if "mongoengine.base.metaclasses" in str(type(col)):
        col = col._collection
    ind = list(col.list_indexes())
#     pp(ind)
    ret = []
    for i in ind:
        ret.append([])
        for s in i['key'].keys():
            if i['key'][s] == -1:
                s = "-" + s
            ret[-1].append(s)
        if len(ret[-1]) == 1:
            ret[-1] = ret[-1][0]
    return ret

def get_indices_meta(o):
    ret = []
    for i in o._meta['index_specs']:
        ret.append([])
        j = i['fields']
        for t in j:
            s = t[0]
            if t[1] == -1:
                s = "-"+s
            ret[-1].append(s)
        if len(ret[-1]) == 1:
            ret[-1] = ret[-1][0]
    return ret

def ensure_indices_meta(o):
    for i in get_indices_meta(o):
        try:
            o.ensure_index(i, background=True)
        except:
            print ("exception -- moving right along")

def index_status():
    return [(x['command']['createIndexes'], x['msg']) for x in db.current_op()['inprog'] if 'msg' in x]

def copy(source,        #must be a collection
         dest,          #db, string, or collection
         resume = False,
         key = None,
         die = 85,
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
    if resume:
        q = dest.objects().order_by("-_id")
        if q.count():
            t = q[0].id
            kw.update({"_id__gte": t})
    print("query:", kw)
    q = source.objects(**kw).order_by("_id")
    # print (type(q), q.count())
    scnt = q.count()
    dcnt = dest.objects.count()
    print ("DBG", source, dest)
    if not key:
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
    else:
        i = key
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
    reptarg = t0
#     every = max(min(500, scnt//10), 1)
    for x in q:
        if die <= psutil.virtual_memory().percent:
            raise Exception("memory limit reached, abort")
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
        newt = time.time()
        if newt >= reptarg or n == scnt:
            reptarg = max(newt, reptarg+1)
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
            sys.stdout.flush()
    print ("")
    sys.stdout.flush()
    for ix in get_indices(source):
        if ix != "_id":
            print ("creating index", "NOT" if not real else "", ix)
            if real:
                dest.create_index(ix)

init()
refresh()
