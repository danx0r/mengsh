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
    _id = me.DynamicField(primary_key = True)

me.connect("test")
print foo.objects
# print foo.objects[0].ass
q = foo.objects(ass__gt = 100)
print q._query
print q[0].ass