`qs` - Query CSV or TSV files directly via SQL
==============================================

This is a Scheme version of [`q`](https://github.com/harelba/q). This works
on Sagittarius Scheme.

Requirements
------------

This script requires the following:

- [Sagittarius Scheme](https://bitbucket.org/ktakashi/sagittarius-scheme)
- [Sagittarius DBD SQLite](https://github.com/ktakashi/sagittarius-dbd-sqlite3)

Example
-------

Querying result of `ps` by PID.

    $ ps | ./qs -O -H "select * from stdin where pid = 1000"
	PID,TTY,TIME,CMD
    1000, ...
	
