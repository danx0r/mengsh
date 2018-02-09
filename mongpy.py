#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import pymongo as pm
import mongoengine as me

try:
    import config
except:
    class _config:
        host = "127.0.0.1"
        port = 27017
        user = ""
        password = ""
    config = _config()

class foo(me.DynamicDocument):         
    pass

me.connect("test")
print foo.objects
# print foo.objects[0].ass
qu = foo.objects(bar__gt = 100)._query
print qu

con=pm.mongo_client.MongoClient()
db = con.test2
q = db.foo2.find(qu)
print q[0], q[0]['bar']
