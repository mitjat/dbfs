dbfs
====

A module for PyFilesystem ([http://code.google.com/p/pyfilesystem/][1]) providing read- and partial write-access to DB fields via a filesystem.

It allows you to mount parts of the DB, one row per file. Changes made to the file will be written back to the DB and vice versa. Creating new files in the mounted filesystem (effectively, inserting rows into the DB) is not supported. 

See class DatabaseFS in `dbfs.py` for details on usage. This is the only file you need to use the library.  
See `putkafs.py` for a more elaborate, real-world example where dbfs was used for a debug-purpose interface to a DB for an online judge (a la topcoder.com) called Putka.

Sample usage:
-------------

Assume we have a sales database `shop` with a table `feedback(id,firstname,lastname,comment,when)`. We want to expose the column `comment` from that table as a series of files.

    from dbfs import DatabaseFS  # this project
    from fs.expose import fuse   # Pyfilesystem 

    # A series of python commands to create the DBcursor object. 
    curcmd = '''
    import psycopg2, psycopg2.extras
    conn = psycopg2.connect(database='shop', user='foo', password='bar')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    '''

    # SQL for getting the full listing of files (including subdirs)
    # in our virtual filesystem
    listcmd = '''
    SELECT
        id AS fid,
        'byname/'||lastname||'_'||firstname AS path
        length(comment) AS size
    FROM feedback;
    '''
    # SQL for moving file contents from and to the DF
    readcmd= "SELECT comment FROM feedback WHERE id=%s"
    writecmd="UPDATE feedback SET comment=%s WHERE id=%s"

    # Test it
    commentfs = DatabaseFS(curcmd,listcmd,readcmd,writecmd)
    fuse.mount(commentfs, '/mnt/customer_feedback')

The above snippet will create a filesystem at /mnt/customer_feedback, containing a folder `byname` which in turn contains a file `surname_lastname` for each client that left a comment. The contents of the file and the `comment` section remain synced for as long as the filesystem is running.

  [1]: http://code.google.com/p/pyfilesystem/