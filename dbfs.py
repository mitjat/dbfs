"""
dbfs
====

A module for PyFilesystem providing read- and partial write-access to DB fields via a filesystem.

It allows you to mount parts of the DB (e.g. column "long_comment" from table "feedback"), one row
per file. Changes made to the file will be written back to the DB, but creating new files in the
mounted filesystem (effectively, isnerting rows into the DB) is not supported.

See class DatabaseFS for information on usage.

Sample usage:
-------------

Assume we have a table 'feedback' with column 'comment' that we want to expose.

# provide commands for connecting to DB & getting relevant info
curcmd = '''
import psycopg2, psycopg2.extras
conn = psycopg2.connect(database='shop', user='foo', password='bar')
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
'''
listcmd = '''
SELECT
  id AS fid,
  'byname/'||lastname||'_'||firstname AS path
  length(comment) AS size
FROM feedback;
'''
readcmd= "SELECT comment FROM feedback WHERE id=%s"
writecmd="UPDATE feedback SET comment=%s WHERE id=%s"

# mount
commentfs = DatabaseFS(curcmd,listcmd,readcmd,writecmd)
fuse.mount(commentfs, '/mnt/customer_feedback')
"""

from fs.base import FS
from fs.errors import *

from StringIO import StringIO
import datetime, time
import os.path

DEBUG = True
def log(*args):
	if DEBUG: print ' '.join(map(str,args))

class DBFile(StringIO):
	"In-memory filelike object for temporarily storing content of an open 'file' from the DB."
	def __init__(self, data, path, mode, filemgr):
		"""
		@param data: initial contents. Ignored if opened in write mode.
		@param path: path to the file
		@param mode: e.g. 'r', 'r+', 'w', 'a'
		@param filemgr: a filesystem (DatabaseFS instance) with at least the following funcitons:
			- flush_callback(path,data): called every time file is flushed
			- close_callback(path): called when file is closed
		"""
		StringIO.__init__(self, data if 'w' not in mode else '')
		if mode=='r' or '+' in mode: self.seek(0)
		self.filemgr = filemgr
		self.path = path
		self.mode = mode
		self.modified = False

	def size(self):
		return len(self.getvalue())

	def close(self):
		self.flush()
		StringIO.close(self)
		self.filemgr.close_callback(self.path)

	def flush(self):
		if self.mode=='r': return
		log('Syncing file',self.path,'to DB')
		if self.modified:
			self.filemgr.flush_callback(self.path, self.getvalue())
			self.modified = False
			log('Synced',self.size(),'bytes')
		else:
			log('Sync skipped -- file %s not changed' % self.path)

	def write(self, data):
		log('Writing',len(data),'bytes to',self.path)
		if self.mode == 'r':
			raise ResourceInvalidError('File is open in read-only mode')
		self.modified = True
		StringIO.write(self, data)
		self.truncate()

		
class DatabaseFS(FS):
	def __init__(self, curcmd, listcmd, readcmd, writecmd):
		"""
		A read-write filesystem backed by DB queries. In other words, you provide an SQL command which decides
		which parts of the database should be visible as files; and SQL commands which determine how to get string
		data from/into the DB (so that changes to mounted files will get written back to DB).
		Not appropriate for huge databases because the listing of all available files is kept in memory (and
		quite frequently re-generated with queries), as are the contents of all open files.

		Each of the three commands described below can be either
		- A string with SQL placeholders (typically '?') for parameters. Parameters will be provided by
			DatabaseFS in the order specified at each command below.
		- A function accepting the parameters specified at each command below, returning a pair
			(query_with_placeholders, tuple_of_parameters).

		For example, the following two values for writecmd are equivalent:
		- "UPDATE people SET comments=? WHERE personid=?"
		- lambda data, fid: ("UPDATE people SET comments=? WHERE personid=?", (data, fid))

		Note however that the lambda (or any other function) must be declared at the top
		level of the program, otherwise it can not be pickled (and thus passed between your
		process and the background mount daemon).
		
		@param curcmd: A string which, when exec-ed (SECURITY!), spawns a local variable 'cur'. 'cur' can be any
			DB cursor adhering to python DB API. The string typically contains an 'import dbmodule' statement and a
			dbmodule.connect(...).cursor() call to get the cursor.
		@param listcmd: SELECT SQL query which retrieves the list of all file paths to create. Paths can be
			relative to the root. Paths can contain '/'. Directories should not be returned, they are
			created implicitly based on full file paths. Each query result row must contain non-NULL
			columns "fid", "path", "size" (in bytes). "fid" is a File ID of any hashable type (e.g. string or int)
			which globally uniquely identifies the underlying table row. However, more than one path can share
			the same fid, very much like hard links on unix. Parameters: none.
		@param readcmd: SELECT SQL query which retrieves the contents of the given file. Parameters: fid.
		@param writecmd: UPDATE SQL query which writes given data to given fid. INSERT-like writing is not
			supported, i.e. you can only modify contents	of files you specified with "listcmd". Paramters: data, fid.
		"""
		self.curcmd = curcmd
		self.listcmd = listcmd
		self.readcmd = readcmd
		self.writecmd = writecmd
		self.openfiles = {} # files that are currently open; maps path -> DBFile object
		self.listing = {}   # list of all directories and files (with full paths) in the FS. maps path -> {'fid':..., 'size':...}
		self.lastUpdate = 0 # last time (timestamp) listing was updated

	def __getattr__(self,attr):
		"""
		A hack to make a database cursor available as self.cur, but not instantiated at object creation time.
		This complication is needed so that instances of this class are picklable and can thus be mounted
		by module fs in the background.
		"""
		if attr!='cur': raise AttributeError
		# instantiate cursor if needed; since we are in __getattr__('cur'), self.cur apparently does not exist yet
		log('Instantiating database cursor')
		exec(self.curcmd)
		self.cur = cur
		return cur
		
	def _getlisting(self):
		"Get a fresh version of self.listing. (Reload it first if too much time has passed since last reload.)"
		if time.time()-self.lastUpdate > 1:
			self.lastUpdate = time.time()
			self.cur.execute(self.listcmd)
			self.listing = dict((self.abspath(str(row['path'])), {'size':row['size'], 'fid':row['fid']}) for row in self.cur)
		return self.listing		

	def flush_callback(self, path, data):
		"Write the file 'path' with contents 'data' back into DB. See DBFile()."
		path = self.abspath(path)
		try: fid = self._getlisting()[path]['fid']
		except KeyError: raise ResourceNotFoundError(path, msg="The underlying DB data haschanged and the file no longer exists.")
		# do the writing
		if hasattr(self.writecmd,'__call__'): args = self.writecmd()
		else: args = (self.writecmd, (data, fid))
		self.cur.execute(*args)
		# error checking
		if self.cur.rowcount==0:
			q = (repr(self.cur.query) if hasattr(self.cur,'query') else repr(args))
			log('!!! Warning: while flushing FID %s: command %s modified no rows, changes have been lost' % (fid, q))

	def close_callback(self, path):
		"Remove fid from list of open files. See DBFile()."
		try: del self.openfiles[path]
		except KeyError: log('!!! Warning: while closing %s: the file was not listed as open in the first place. Known open files: %s' % (path, self.openfiles.keys()))

	def abspath(self, path):
		"Absolutize and normalize the path. Relative paths are taken relative to '/'"
		return os.path.normpath('/'+path.lstrip('/'))

	### Functions below override stubs in base class and provide basic filesystem functionality.
	### See base class documentation.
	
	def open(self, path, mode='r'):
		path = self.abspath(path)
		try: fid = self._getlisting()[path]['fid']
		except KeyError: raise ResourceNotFoundError(path)
		log('Opening file %s (FID %s) in mode %s' % (path, fid, mode))
		# read contents
		if hasattr(self.readcmd,'__call__'): args = self.readcmd
		else: args = (self.readcmd, (fid,))
		self.cur.execute(*args)
		# error checking 
		q = (repr(self.cur.query) if hasattr(self.cur,'query') else repr(args))
		if self.cur.rowcount==0:
			msg = '(Writing is only supported to existing files)' if mode=='w' else ''
			raise ResourceNotFoundError('Query %s returned no results %s' % (repr(cur.command), msg))
		if self.cur.rowcount>1:
			raise ResourceNotFoundError('Query %s matches more than one row' % repr(cur.command))
		# create file object
		data = self.cur.fetchone()[0]
		s = DBFile(data, path, mode, self)
		self.openfiles[path] = s
		return s

	def isfile(self, path):
		path = self.abspath(path)
		return path in self._getlisting()

	def isdir(self, path):
		# Directories only exist implicitly, if there is a file contained in them
		path = self.abspath(path)+'/'
		if path=='//': path='/'
		return any(fn.startswith(path) for fn in self._getlisting())

	def listdir(self, path='./', wildcard=None, full=False, absolute=False, dirs_only=False, files_only=False):
		path = self.abspath(path)+'/'
		if path=='//': path='/'
		log('Listing dir %s (wildcard=%s, full=%s, absolute=%s, dirs_only=%s, files_only=%s' % (path, wildcard, full, absolute, dirs_only, files_only))
		subtree = [fn[len(path):] for fn in self._getlisting() if fn.startswith(path)]
		children = [fn for fn in subtree if '/' not in fn]
		if not files_only:
			# add implicit directories
			children.extend(set(fn.split('/',1)[0]+'/' for fn in subtree if '/' in fn)) 
		if dirs_only:
			children = [fn for fn in children if fn.endswith('/')]
		if wildcard:
			children = [fn for fn in children if fnmatch(fn, wildcard)]
		if absolute:
			children = [self.abspath(path+'/'+fn) for fn in children]
		# remove trailing slash which temporarily marked directories
		children = [fn.rstrip('/') for fn in children]

		log('Returning', children)
		return children

	def getinfo(self, path):
		log('getinfo', path)
		path = self.abspath(path)
		info = {}
		info['created_time'] = info['modified_time'] = info['accessed_time'] = datetime.date(1970,1,1)
		if self.isfile(path):
			try: f = self._getlisting()[path]
			except: raise ResourceNotFoundError(path)
			if path in self.openfiles:
				info['size'] = self.openfiles[path].size()
			else:
				info['size'] = f['size']
			info['st_mode'] = 0644
		elif self.isdir(path):
			info['st_mode'] = 0755
		else:
			raise ResourceNotFoundError(path)
		return info
