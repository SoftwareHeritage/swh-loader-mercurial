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

send_contents: True
send_directories: True
send_revisions: True
send_releases: True
send_occurrences: True
content_packet_size: 1000
content_packet_size_bytes: 1073741824
directory_packet_size: 2500
revision_packet_size: 1000
release_packet_size: 1000
occurrence_packet_size: 1000
```

# Basic use

From python3's toplevel:

## Remote (failure)

``` Python
project = 'hello'
# remote repository
origin_url = 'https://www.mercurial-scm.org/repo/%s' % project
# local clone
directory = '/home/storage/hg/repo/%s' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import LoadMercurialTsk

t = LoadMercurialTsk()
t.run(origin_url=origin_url, directory=directory, visit_date='2016-05-03T15:16:32+00:00')
```

## local directory (failure)

Only origin, contents, and directories are filled so far.

Remaining objects are empty (revision, release, occurrence).

``` Python
project = '756015-ipv6'
directory = '/home/storage/hg/repo/%s' % project
origin_url = 'https://%s.googlecode.com' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import SlowLoadMercurialTsk

t = SlowLoadMercurialTsk()
t.run(origin_url=origin_url, directory=directory, visit_date='2016-05-03T15:16:32+00:00')
```

## local archive (failure)

``` Python
project = '756015-ipv6-source-archive.zip'
archive_path = '/home/storage/hg/repo/%s' % project
origin_url = 'https://%s-archive.googlecode.com' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import SlowLoadMercurialArchiveTsk

t = SlowLoadMercurialArchiveTsk()
t.run(origin_url=origin_url, archive_path=archive_path, visit_date='2016-05-03T15:16:32+00:00')
```
