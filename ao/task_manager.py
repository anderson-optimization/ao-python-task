import json
import os.path
from mimetypes import guess_type
from stat import * # ST_SIZE etc

from util import set_param

def get_columns(df):
	print df.columns

class FileOutput:
	def __init__(self,
			file_path=None,
			item_id=None,
			name=None,
			tid=None,
			overwrite=None):
		if not file_path:
			raise ValueError("Must provide a filepath")
		if not item_id:
			raise ValueError("Must provide an item id")
		if not name:
			raise ValueError("Must provide a name for ao item")

		self.file_path=file_path
		self.item_id=item_id
		self.name=name

		self.tid=tid

		self.filename=os.path.basename(file_path)
		self.item={
			"id": item_id
		}
		set_param(self.item,'name.name',name)
		self.overwrite=overwrite

	def dump_json(self,data,indent=4):
		with open(self.file_path,'w') as outfiledata:
			json.dump(data,outfiledata,indent=indent)

	def set_meta_data(self):
		st = os.stat(self.file_path)
		mime_type=guess_type(self.file_path)[0]
		encoding=guess_type(self.file_path)[1]
		set_param(self.item,'meta.lastModifed',st[ST_MTIME])
		set_param(self.item,'meta.extension','csv')
		set_param(self.item,'meta.name',self.filename)
		set_param(self.item,'meta.size',st[ST_SIZE])
		set_param(self.item,'meta.type',mime_type)

	def get_item(self):
		return self.item

	def get_file_ref(self):
		file_ref={
			"path":self.file_path,
			"item":self.item
		}
		if self.tid:
			file_ref['tid']=self.tid
		if self.overwrite:
			file_ref['overwrite']=True

		return file_ref

class ProjectOutput:
	def __init__(self,item=None,tid=None,overwrite=None):

		print "ProjectRefConstructor"

		if not item:
			raise ValueError("Must provide a project item")

		if not tid:
			raise ValueError("Must provide a team id (tid)")

		self.tid=tid
		self.item=item
		self.overwrite=overwrite

		if 'type' not in item:
			raise ValueError("Item must have a type")

		if not item['type'].startswith('project'):
			raise ValueError("Item must be a project type")

	def get_ref(self):
		ref={
			"item":self.item
		}
		if self.tid:
			ref['tid']=self.tid
		if self.overwrite:
			ref['overwrite']=True

		return ref



class DataOutput:
	def __init__(self,
			filename=None,
			df=None,
			columns=None,
			name=None,
			item_id=None,
			item_type="data",
			tags=None,
			tid=None,
			stringify=False,
			overwrite=True):

		print "DataRefConstructor"

		if df.empty:
			raise ValueError("Must provide a dataframe for output")
		
		if not filename:
			raise ValueError("Must provide a filename to store the data object")

		if not columns:
			raise ValueError("Must provide a column mapping")

		self.filename=filename
		self.df=df
		self.columns=columns

		self.item_id=item_id
		self.item_type=item_type
		self.name=name
		self.tid=tid
		self.tags=tags

		self.stringify=stringify
		self.overwrite=overwrite


	def write_to_fs(self):
		print "Write data to filesystem"
		print self.df.head()
		print self.df.shape
		data_out=self.df.to_dict('records')
		with open(self.filename,'w') as outfile:
			json.dump(data_out,outfile)


	def get_item(self):
		item={
			"type":self.item_type,
			"columns":self.columns
		}
		if self.item_id:
			item['id']=self.item_id
		if self.name:
			set_param(item,'name.name',self.name)
		if self.stringify:
			set_path(item,'body.stringify',True)
		if self.tags:
			item['tags']=self.tags

		return item
		get_columns(self.df)

	def get_data_ref(self):
		data_ref={
			"body":self.filename,
			"item":self.get_item()
		}
		if self.tid:
			data_ref['tid']=self.tid
		if self.overwrite:
			data_ref['overwrite']=True

		return data_ref


class Output:
	def __init__(self):
		print "Output Constructor"
		self.data_items=[]
		self.files=[]
		self.projects=[]
		self.data=None
		self.output=None

	def add_data_item(self,data_item):
		self.data_items.append(data_item)


	def add_file(self,file):
		self.files.append(file)

	def set_data(self,data):
		self.data=data

	def add_project(self,project):
		try:
		    ref=project.get_ref()
		except AttributeError:
			ref=project
		self.projects.append(ref)
		    


	def get_output_obj(self):
		output={}

		if self.data:
			output['data']=self.data

		if len(self.data_items)>0:
			output['dataitem']=self.data_items

		if len(self.files)>0:
			output['file']=self.files

		if self.output:
			output['output']=self.output

		if self.projects:
			output['project']=self.projects

		return json.dumps(output)

	def __str__(self):
		return self.get_output_obj()