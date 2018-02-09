#
# Set of utilities to manage multiple mongodb databases, collections, and hosts
#
import argparse
import pymongo
import mongoengine as meng
from pp import pp

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument('--host', default = "127.0.0.1")    
parser.add_argument('--host1')
parser.add_argument('--host2')
parser.add_argument('--host3')    

args = parser.parse_args()

for host in ['host', 'host1', 'host2', 'host3']:
    mongo = getattr(args, host)
    if not mongo:
        continue
    if mongo.find("mongodb://") != 0:
        mongo = "mongodb://127.0.0.1:27017/" + mongo
    print mongo
    print meng.connect(host = mongo, alias=host)


# class foo1(meng.DynamicDocument):
#     meta = {
#         'collection': 'foo',
#         'db_alias': 'host'
#     }
#     
# class foo2(meng.DynamicDocument):
#     meta = {
#         'collection': 'foo',
#         'db_alias': 'host2'
#     }
# 
# print "1", foo1.objects[0].arse
# print "2", foo2.objects[0].arse

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
    classname = kw.keys()[0]
    collectionname = classname
    host = kw[classname]
    hostname = 'host'
    if host > 0:
        classname += "_%s" % host
        hostname += "%s" % host
    exe = create_template.format(classname, collectionname, hostname)
    print exe                        
    exec(exe)

create(foo = 0)
create(foo = 2)
print foo.objects
print foo_2.objects
