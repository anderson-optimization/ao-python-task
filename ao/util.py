import os, os.path
from mimetypes import guess_type
from stat import * # ST_SIZE etc

def get_path(obj,path):
    keys = path.split('.')
    for k in keys:
        try:
            obj = obj[k]
        except KeyError:
            return None
    return obj

def get_param(obj,path):
    try:
        obj = obj['parameter']
    except KeyError:
        return None
    return get_path(obj,path)

def set_path(obj,path,value):
	keys = path.split('.')
	for k in keys[:-1]:
		if k not in obj.keys():
			obj[k] = {}
		obj = obj[k]
	obj[keys[-1]] = value

def set_param(obj,path,value):
    if 'parameter' not in obj.keys():
        obj['parameter']={}
    set_path(obj['parameter'],path,value)

def get_file_item(file_path,itemid,name,filename):
    item={
        "id": itemid,
        "parameter":{
            "name": {
                "name": name
            },
            "meta":{}
        }
    }   

    st = os.stat(file_path)
    mime_type=guess_type(file_path)[0]
    encoding=guess_type(file_path)[1]
    fn, file_extension = os.path.splitext(file_path)
    set_param(item,'meta.lastModifed',st[ST_MTIME])
    set_param(item,'meta.extension',file_extension)
    set_param(item,'meta.name',filename)
    set_param(item,'meta.size',st[ST_SIZE])
    set_param(item,'meta.type',mime_type)
    return item