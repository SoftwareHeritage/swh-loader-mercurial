swh-loader-mercurial
=========================

The Software Heritage Mercurial Loader is a tool and a library to walk a local mercurial
repository and inject into the SWH dataset all contained files that weren't known
before.

The main entry points are:
- :class:`swh.loader.mercurial.loader.HgBundle20Loader` which reads and loads a remote
  repository (or local).

- :class:`swh.loader.mercurial.from_disk.HgLoaderFromDisk` which reads and loads a local
  repository into an SWH archive.

- :class:`swh.loader.mercurial.from_disk.HgArchiveLoaderFromDisk` which reads and loads
  a local repository wrapped within a tarball

# CLI run

## Configuration file

/tmp/mercurial.yml:
``` YAML
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```

## Basic use

``` bash
swh loader --C /tmp/mercurial.yml run mercurial https://www.mercurial-scm.org/repo/hello
```
or `mercurial_from_disk`
