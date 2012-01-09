#!/usr/bin/python

# run with --help to get usage info
"""./%prog [options]

[Un]mount parts of putka4 DB as an editable filesystem.
This gives direct access to the DB, intended for admins only."""


# Options
# =======

import optparse, sys, os

# built-in defaults
mountpoint = "/home/putka4/putkafs"
dbname = "putka4"
dbuser = ""
dbpass = ""
# try to get better defaults from django config
try:
	sys.path.insert(0,os.path.dirname(os.path.abspath(__file__ if '__file__' in globals() else '.'))+'/../../ui')
	import settings
	db = settings.DATABASES['default']
	dbname, dbuser, dbpass = db['NAME'], db['USER'], db['PASSWORD']
	mountpoint = os.path.normpath(settings.BASE_DIR+'/../putkafs') #BASE_DIR referes to UI
finally:
	del sys.path[0]

parser = optparse.OptionParser(usage=__doc__, epilog='Note: option defaults are pulled from django config (if found).')
parser.add_option("-m", "--mountpoint", help="Directory on which to [un]mount putkafs. Default: %default.", default=mountpoint)
parser.add_option("-d", "--dbname", help="Database to connect to. Default: %default.", default=dbname)
parser.add_option("-U", "--dbuser", help="Database user. Default: "+(dbuser if dbuser else 'current unix user.'), default=dbuser)
parser.add_option("-P", "--dbpass", help="Database password for user. Default: none.", default=dbpass)
parser.add_option("-N", "--no-background", dest="bg", action="store_false", help="Run in the console, not as a daemon. Default: false.", default=True)
parser.add_option("-u", "--unmount", action="store_true", help="Unmount putkafs.", default=False)
(opts, args) = parser.parse_args()

if len(args) != 0:
	parser.error("Unknown arguments: "+repr(args))
opts.mountpoint = os.path.abspath(opts.mountpoint)
if not os.path.isdir(opts.mountpoint) or (not opts.unmount and os.listdir(opts.mountpoint)!=[]):
	print 'Mountpoint %s does not exist or is not empty.' % opts.mountpoint
	sys.exit(1)

# Program
# =======
	
# Allow importing modules from this script's directory
sys.path.append('.')
# chdir to this script's directory. This is important for child processes (one for each
# dbfs mount in the background) since they have to be able to find dbfs.py
if '__file__' in globals():
	scriptdir = os.path.dirname(os.path.abspath(__file__))
	os.chdir(scriptdir)

import dbfs; reload(dbfs);
from dbfs import DatabaseFS
from fs.mountfs import MountFS						
from fs.expose import fuse

import signal, time
import tempfile
import traceback


# the command for getting a DB cursor
curcmd = """
import psycopg2, psycopg2.extras
conn = psycopg2.connect(%s %s %s)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
""" % (
	('database=%s,' % repr(opts.dbname)) if opts.dbname else '',
	('user=%s,' % repr(opts.dbuser)) if opts.dbuser else '',
	('password=%s,' % repr(opts.dbpass)) if opts.dbpass else ''
)

# create database filesystems
fs_uploads = DatabaseFS(
	curcmd,
	listcmd= """
		(SELECT
			id AS fid,
			'id/'||id AS path,
			length(source) AS size
			FROM results_upload)
		UNION
		(SELECT
			results_upload.id AS fid,
			'user/' || auth_user.username || '/' || lpad(results_upload.id::text,4,'0') || '-' || tasks_task.url AS path,
			length(results_upload.source) AS size
			FROM results_upload
			JOIN tasks_task ON (tasks_task.id=results_upload.task_id)
			JOIN auth_user ON (auth_user.id=results_upload.user_id));
	""",
	readcmd= "SELECT source FROM results_upload WHERE id=%s",
	writecmd="UPDATE results_upload SET source=%s WHERE id=%s",
	)
fs_attachments = DatabaseFS(
	curcmd,
	listcmd= """
		(SELECT
			id AS fid,
			'id/'||id AS path,
			length(data) AS size
			FROM tasks_file)
		UNION
		(SELECT
			tasks_file.id AS fid,
			'task/' || tasks_task.url || '/' || tasks_file.filename AS path,
			length(tasks_file.data) AS size
			FROM tasks_file
			JOIN tasks_task ON (tasks_task.id=tasks_file.task_id));
	""",
	readcmd= "SELECT data FROM tasks_file WHERE id=%s",
	writecmd="UPDATE tasks_file SET source=%s WHERE id=%s"
	)
fs_testscripts = DatabaseFS(
	curcmd,
	listcmd= """
		(SELECT
			id AS fid,
			'id/'||id AS path,
			length(testscript) AS size
			FROM tasks_task)
		UNION
		(SELECT
			id AS fid,
			'task/'||url AS path,
			length(testscript) AS size
			FROM tasks_task);
	""",
	readcmd= "SELECT testscript FROM tasks_task WHERE id=%s",
	writecmd="UPDATE tasks_task SET testscript=%s WHERE id=%s",
	)

# create putkafs -- a virtual filesystem with filesystems above mounted onto its folders
putkafs = MountFS()
putkafs.mountdir('uploads', fs_uploads)
putkafs.mountdir('att', fs_attachments)
putkafs.mountdir('testscript', fs_testscripts)


if opts.unmount:
	print 'Unmounting putkafs from %s' % opts.mountpoint
	ret = os.system('fusermount -u %s' % opts.mountpoint)
	sys.exit(ret)

else:
	# redirect stdout, stderr to a temporary file
	if opts.bg:
		sys.stdout.flush()
		sys.stderr.flush()
		_out, _err = os.dup(sys.stdout.fileno()), os.dup(sys.stderr.fileno())
		temp_fd, temp_path = tempfile.mkstemp(prefix='putka.',suffix='.tmp',text=True)
		temp_file = os.fdopen(temp_fd, 'a+', 0);
		os.dup2(temp_file.fileno(), sys.stdout.fileno())
		os.dup2(temp_file.fileno(), sys.stderr.fileno())

	# perform the mount (starts a background process)
	mount_ok = False
	try:
		if not opts.bg:
			print 'Mount process started in foreground. If no error messages appear, everything is in order.'
			print 'Do NOT put this process in the background. To unmount, run putkafs.py FROM ANOTHER CONSOLE.'
		mp = fuse.mount(putkafs, opts.mountpoint, foreground=not opts.bg)
		mount_ok = True
	except:
		traceback.print_exc()

	if opts.bg:
		# Restore stdout, stderr. The mount process branched away and inherited the temp file as stdout/err.
		os.dup2(_out, sys.stdout.fileno())
		os.dup2(_err, sys.stderr.fileno())

		# Print status
		if mount_ok:
			print 'Mounted putkafs at %s' % opts.mountpoint
		else:
			print 'Error mounting putkafs.'
			print open(temp_path).read().rstrip()
			try: os.remove(temp_path)
			except: pass

