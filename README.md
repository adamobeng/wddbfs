wddbfs is a Python package which implements webdavfs that exposes the contents of sqlite databases to the filesystem

# Installation

`pip install git+https://github.com/adamobeng/wddbfs`

# Usage

```
usage: wddbfs [-h] [-c CONFIG] [--host HOST] [--port PORT] [--log-level LOG_LEVEL] [--formats FORMATS] [--timeout TIMEOUT] [--anonymous] [--username USERNAME] [--password PASSWORD] [--db-path DB_PATH [DB_PATH ...]] [--allow-abspath]

options:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        config file path
  --host HOST
  --port PORT
  --log-level LOG_LEVEL
  --formats FORMATS
  --timeout TIMEOUT
  --anonymous           allow access without authentication
  --username USERNAME
  --password PASSWORD
  --db-path DB_PATH [DB_PATH ...]
                        paths to sqlite database files
  --allow-abspath       make it possible to access any database on the host filesystem by specifying its absolute path relative to the WebDAV root (e.g. /mount/webdav/absolute/path/on/host/fs/to/db.sqlite)

Args that start with '--' (eg. --host) can also be set in a config file (specified via -c). Config file syntax allows: key=value, flag=true, stuff=[a,b,c] (for details, see syntax at https://goo.gl/R74nmi). If an arg is specified in
more than one place, then commandline values override config file values which override defaults.
```

More information [in this blog post](https://adamobeng.com/wddbfs-mount-a-sqlite-database-as-a-filesystem/).
