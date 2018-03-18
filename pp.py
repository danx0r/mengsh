#
# pretty-print a JSON type dict/list:
#
from __future__ import print_function
import sys, io, traceback
from datetime import datetime
from pprint import pprint

try:
    from urllib.parse import unquote      #py3
except:
    from urllib import unquote            #py2

fout = sys.stdout

MAXLISTLEN = 10
MAXSTRLEN = 200

GREEN = '\x1b[1;32m%s\x1b[0m'


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
    return type(x) in (str, str)

def is_listy(x):
    if type(x) == list:
        return True
    if type(x) == tuple:
        return True
    if 'List' in str(type(x)):
        return True
    return False

def is_dicty(x):
#     if type(x) == dict:
#         return True
#     if 'Dict' in str(type(x)):
#         return True
#     return False
    return hasattr(x, 'keys')

INDENT = 4
def pp_json_dict(d, indent, typ=None, maxlist=MAXLISTLEN):
#     print >> sys.stderr, "DEBUG PP dict:", type(d), d, indent, typ
    if len(d) == 0:
        print(" " * indent + "{},", file=fout)
        return
    print(" " * indent + "{", file=fout)
    keys = list(d.keys())
    keys.sort()
    for key in keys:
        s = " " * (indent+INDENT) + ((".%s" % key) if typ=="obj" else ("'%s'" % key) if is_stringy(key) else ("%s" % key)) + ":"
        if fout == sys.stdout:
            print(s, end=' ', file=fout)
        else:
            print(s, end=' ', file=fout)
        val = d[key]
        if is_atomic(val):
            pp_json_atom(val)
        else:
            if len(val):
                print("", file=fout)
            pp(val, indent+INDENT, maxlist=maxlist)
    print(" " * indent + "},", file=fout)

def pp_json_list(d, indent, maxlist = MAXLISTLEN):
    if len(d) == 0:
        print("[]", file=fout)
        return
    j = 0
    all_atomic = True
    for val in d[:maxlist]:
        if not is_atomic(val):
            all_atomic = False
    print(" " * indent + "[", file=fout, end='' if all_atomic else '\n')
    for key in range(len(d)):
        if j >= maxlist:
            s = GREEN % ("<+%d more>" % (len(d)-maxlist))
            if all_atomic:
                print(s, file=fout, end='')
            else:
                print((" " * (indent+INDENT))[:-1], s, file=fout)
            break
        j += 1
        val = d[key]
        if is_atomic(val):
            if not all_atomic:
                print((" " * (indent+INDENT))[:-1], end=' ', file=fout)
            pp_json_atom(val, cr = False if all_atomic else True)
        else:
            pp(val, indent+INDENT, maxlist=maxlist)
    print(" " * (0 if all_atomic else indent) + "],", file=fout)

def pp_json_atom(val, cr = True):
#     print "DEBUG PP atom:", type(val), val
    if type(val) in (int, int, float, str, str, bool, datetime, type(None)):
        s = ""
        if is_stringy(val):
            if val.find("http://") == 0:
#                 print "DEBUG url decode raw:", type(val), val
                val = unquote(str(val)).encode('latin-1')         #wtf is up with dat
            if len(val) > MAXSTRLEN:
                val = val[:MAXSTRLEN] + GREEN % (" <+%d chars>" % (len(val)-MAXSTRLEN))
            if '"' in val:
                val = "'"+val+"'"
            else:
                val = '"'+val+'"'
            s += " " + val + ","
        else:
            s += " " + str(val) + ","
        print(s, end=' ', file=fout)
    else:
        if "bson.objectid" in str(type(val)):
            print(val, end=' ', file=fout)
        else:
            print(type(val), end=' ', file=fout)
            try:
                print(len(val), "bytes", file=fout)
            except:
                print("", file=fout)
    if cr:
        print(file=fout)
    
def pp(j, indent=0, as_str=False, maxlist=MAXLISTLEN):
#     print ("DEBUG PP list:", maxlist)
    global fout
    if as_str:
        fout = io.StringIO()
        try:
            pp(j, indent=indent, as_str=False, maxlist=maxlist)
        except:
            print("-----------error in pp-----------")
            print(traceback.format_exc(), file=sys.stderr)
        ret = fout.getvalue()
#         try:
#             ret = ret.encode('utf-8')
#         except:
#             ret = ret.decode('latin-1').encode('utf-8')
        fout = sys.stdout
        return ret
#     print "DEBUG pp top:", type(j), j, indent
    if is_dicty(j):
        pp_json_dict(j, indent, typ="dict", maxlist=maxlist)
    elif is_listy(j):
        pp_json_list(j, indent, maxlist=maxlist)
    elif str(type(j)) == "<type 'collections.defaultdict'>":
        pp_json_dict(dict(j), indent, typ="dict", maxlist=maxlist)
    else:
        try:
            pp_json_dict(j._data, indent, typ="obj", maxlist=maxlist)
        except:
            try:
                if j.count():
                    pp_json_list(list(j), indent, maxlist=maxlist)
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
    print("-----------back from pp-----------")
    print(s)
    pp([1,2,3,{}])
    pp([1,2,3,{1:23}])
    s=pp([1,2,3,{}],as_str=True)
    print("-----------back from pp-----------")
    print(s)
    s=pp([1,2,3,{1:23}],as_str=True)
    print("-----------back from pp-----------")
    print(s)
    pp([1,2,3,4,5,6,7,7,6,5,4,3,2,1])
    