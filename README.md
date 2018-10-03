swh-loader-mercurial
=========================

# Configuration file

In usual location for a loader, *{/etc/softwareheritage/ | ~/.swh/ |
~/.config/swh/}loader/hg.yml*:

``` YAML
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```

# Basic use

The main entry point to import a Mercurial repository is the `main` function
defined in the `swh.loader.mercurial.cli` module:

``` bash
python3 -m swh.loader.mercurial.cli
```


If the Python package has been installed via `pip`, you should be able
to type:

``` bash
user@host:~$ swh-loader-hg --help

Usage: swh-loader-hg [OPTIONS] ORIGIN_URL

Options:
  -d, --hg-directory TEXT         Path to the hg (local) directory to load
                                  from. If unset, the hg repo will ben cloned
                                  from the given (origin) url
  -a, --hg-archive TEXT           Path to the hg (local) archive file to load
                                  from.
  -D, --visit-date TEXT           Visit date (defaults to now)
  -l, --log-level [NOTSET|DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Log level
  --help                          Show this message and exit.

```

For example:

``` bash
user@host:~$ swh-loader-hg https://www.mercurial-scm.org/repo/hello
[...]
```


# From Python
From python3's toplevel:

## Remote

``` Python
project = 'hello'
# remote repository
origin_url = 'https://www.mercurial-scm.org/repo/%s' % project
# local clone
directory = '/home/storage/hg/repo/%s' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import LoadMercurial

t = LoadMercurial()
t.run(origin_url=origin_url, directory=directory, visit_date='2016-05-03T15:16:32+00:00')
```

## local directory

Only origin, contents, and directories are filled so far.

Remaining objects are empty (revision, release, occurrence).

``` Python
project = '756015-ipv6'
directory = '/home/storage/hg/repo/%s' % project
origin_url = 'https://%s.googlecode.com' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import LoadMercurial

t = LoadMercurial()
t.run(origin_url=origin_url, directory=directory, visit_date='2016-05-03T15:16:32+00:00')
```

## local archive

``` Python
project = '756015-ipv6-source-archive.zip'
archive_path = '/home/storage/hg/repo/%s' % project
origin_url = 'https://%s-archive.googlecode.com' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import LoadArchiveMercurial

t = LoadArchiveMercurial()
t.run(origin_url=origin_url, archive_path=archive_path, visit_date='2016-05-03T15:16:32+00:00')
```
