#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import pymongo as pm
import mongoengine as me

class dyndoc(me.DynamicDocument):         
    pass
dyndoc_instance = dyndoc()

def make_query(*args, **kw):
    qu = me.Q(**kw).to_query(dyndoc_instance)
    return qu

qu = make_query(bar__gt = 222)
con=pm.mongo_client.MongoClient()
db = con.test2
q = db.foo2.find(qu)
print q[0]
