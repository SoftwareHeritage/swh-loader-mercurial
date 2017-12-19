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

``` Python
project = '756015-ipv6'
directory = '/home/storage/hg/repo/%s' % project
origin_url = 'https://%s.googlecode.com' % project

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.mercurial.tasks import LoadMercurialTsk

t = LoadMercurialTsk()
t.run(origin_url=origin_url, directory=directory, visit_date='2016-05-03T15:16:32+00:00')
```
