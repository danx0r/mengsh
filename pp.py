#
# pretty-print a JSON type dict/list:
#
import sys, urllib, StringIO, traceback
from datetime import datetime
from pprint import pprint
fout = sys.stdout

def is_atomic(x):
#     return type(x) in (str, unicode, int, float, bool, datetime, type(None))
    if 'mongoengine' in str(type(x)):
        return False
    if hasattr(x, '_data'):
        return False
    if is_listy(x):
        return False
    if is_dicty(x):
        return False
    return True

def is_stringy(x):
    return type(x) in (str, unicode)

def is_listy(x):
    if type(x) == list:
        return True
    if type(x) == tuple:
        return True
    if 'List' in str(type(x)):
        return True
    return False

def is_dicty(x):
    if type(x) == dict:
        return True
    if 'Dict' in str(type(x)):
        return True
    return False

INDENT = 4
def pp_json_dict(d, indent, typ=None):
#     print >> sys.stderr, "DEBUG PP dict:", type(d), d, indent, typ
    if len(d) == 0:
        print >> fout, "{}"
        return
    print >> fout, " " * indent + "{"
    keys = d.keys()
    keys.sort()
    for key in keys:
        if fout == sys.stdout:
            print >> fout, " " * (indent+INDENT) + (("." + key) if typ=="obj" else ("'" + key + "'")) + ":",
        else:
            print >> fout, (" " * (indent+INDENT) + (("." + key) if typ=="obj" else ("'" + key + "'")) + ":").encode('utf-8'),
        val = d[key]
        if is_atomic(val):
            pp_json_atom(val)
        else:
            if len(val):
                print >> fout, ""
            pp(val, indent+INDENT)
    print >> fout, " " * indent + "},"

def pp_json_list(d, indent):
#     print "DEBUG PP list:", type(d), d, indent
    if len(d) == 0:
        print >> fout, "[]"
        return
    print >> fout, " " * indent + "["
    for key in range(len(d)):
        val = d[key]
        if is_atomic(val):
            print >> fout, (" " * (indent+INDENT))[:-1],
            pp_json_atom(val)
        else:
            pp(val, indent+INDENT)
    print >> fout, " " * indent + "],"

def pp_json_atom(val):
#     print "DEBUG PP atom:", type(val), val
    if type(val) in (int, long, float, str, unicode, bool, datetime, type(None)):
        s = ""
        if is_stringy(val):
            if val.find("http://") == 0:
#                 print "DEBUG url decode raw:", type(val), val
                val = urllib.unquote(unicode(val)).encode('latin-1')         #wtf is up with dat
            if '"' in val:
                val = "'"+val+"'"
            else:
                val = '"'+val+'"'
            s += " " + val + ","
        else:
            s += " " + str(val) + ","
        if fout == sys.stdout:
            try:
                print >> fout, s
            except:
                print >> fout, s.encode('utf-8')
        else:
            ### dbm removed attempt to clean up unicode handling
#             try:
#                 s = s.encode('utf-8')
#             except:
#                 s = s.decode('latin-1').encode('utf-8')
            print >> fout, s
    else:
        if "bson.objectid" in str(type(val)):
            print >> fout, val
        else:
            print >> fout, type(val),
            try:
                print >> fout, len(val), "bytes"
            except:
                print >> fout, ""
    
def pp(j, indent=0, as_str=False):
    global fout
    if as_str:
        fout = StringIO.StringIO()
        try:
            pp(j, indent=indent, as_str=False)
        except:
            print "-----------error in pp-----------"
            print >> sys.stderr, traceback.format_exc()
        ret = fout.getvalue()
        try:
            ret = ret.encode('utf-8')
        except:
            ret = ret.decode('latin-1').encode('utf-8')
        fout = sys.stdout
        return ret
#     print "DEBUG pp top:", type(j), j, indent
    if is_dicty(j):
        pp_json_dict(j, indent, typ="dict")
    elif is_listy(j):
        pp_json_list(j, indent)
    elif str(type(j)) == "<type 'collections.defaultdict'>":
        pp_json_dict(dict(j), indent, typ="dict")
    else:
        try:
            pp_json_dict(j._data, indent, typ="obj")
        except:
            try:
                if j.count():
                    pp_json_list(list(j), indent)
            except:
                pprint(j, fout)

if __name__ == "__main__":
#     u = u'\u8349\u67f3\u8bba\u575b\u5730\u5740'
    u = '\xe1'
    x = {u:u,'zzz':123456, 'aaa':"simple strings"}
#     x['zz-top'] = {'subdict:': u'\u575b\u5730'}
#     x['listless'] = [1,2,3]
#     x['bad_ascii'] = u'\xa0'
    s = pp(x, as_str=True)
    print "-----------back from pp-----------"
    print s
