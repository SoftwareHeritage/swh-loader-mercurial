# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import code
import datetime
import hglib
import logging
import os
import random
import sys
import time
import urllib.parse

from binascii import hexlify, unhexlify
from swh.model.hashutil import MultiHash
from ..converters import PRIMARY_ALGO as ALGO
from ..objects import SimpleTree

from swh.loader.core.tests import BaseLoaderTest

from .common import HgLoaderMemoryStorage


class HgLoaderValidater:
    """Loader validater

    """
    def __init__(self, loader):
        self.loader = loader

    def generate_all_blobs(self, validate=True, frequency=1):
        logging.debug('GENERATING BLOBS')
        i = 0
        start = time.time()
        u = set()
        for blob, node_info in self.loader.br.yield_all_blobs():
            filename = node_info[0]
            header = node_info[2]
            i += 1

            hashes = MultiHash.from_data(blob, hash_names=set([ALGO])).digest()
            bhash = hashes[ALGO]
            self.file_node_to_hash[header['node']] = bhash

            u.update([bhash])

            if validate:
                if random.random() < frequency:
                    self.validate_blob(filename, header, blob)

            if i % 10000 == 0:
                logging.debug(i)

        logging.debug('\nFOUND %s BLOBS' % i)
        logging.debug('FOUND: %s UNIQUE BLOBS' % len(u))
        logging.debug('ELAPSED: %s' % (time.time()-start))

    def validate_blob(self, filename, header, blob):
        if not self.hg:
            self.hg = hglib.open(self.hgdir)

        data = bytes(blob)

        filepath = os.path.join(self.hg.root(), bytes(filename))
        linknode = hexlify(header['linknode'])
        cat_contents = self.hg.cat([filepath], rev=linknode)

        if cat_contents != data:
            logging.debug('INTERNAL ERROR ERROR ERROR ERROR')
            logging.debug(filename)
            logging.debug(header)
            logging.debug('-----')
            logging.debug(cat_contents)
            logging.debug('---- vs ----')
            logging.debug(data)
            code.interact(local=dict(globals(), **locals()))
            quit()
        else:
            logging.debug('v', end='')

    def generate_all_trees(self, validate=True, frequency=1):
        logging.debug('GENERATING MANIFEST TREES')

        c = 0
        n = 0
        u = set()

        start = time.time()
        validated = 0

        for header, tree, new_dirs in self.loader.load_directories():
            if validate and (c >= validated) and (random.random() < frequency):
                self.validate_tree(tree, header, c)

            for d in new_dirs:
                u.add(d['id'])

            c += 1
            n += len(new_dirs)

            logging.debug('.', end='')
            if c % 20 == 0:
                sys.stdout.flush()
            if c % 10000 == 0:
                logging.debug(c)

        logging.debug('\nFOUND: %s COMMIT MANIFESTS' % c)
        logging.debug('FOUND: %s NEW DIRS' % n)
        logging.debug('FOUND: %s UNIQUE DIRS' % len(u))
        logging.debug('ELAPSED: %s' % (time.time()-start))

    def validate_tree(self, tree, header, i):
        if not self.hg:
            self.hg = hglib.open(self.hgdir)

        commit_id = header['linknode']
        if len(commit_id) == 20:
            commit_id = hexlify(commit_id)

        base_tree = SimpleTree()
        base_files = list(self.hg.manifest(rev=commit_id))
        bfiles = sorted([f[4] for f in base_files])

        for p in base_files:
            base_tree.add_blob(
                p[4], self.file_node_to_hash[unhexlify(p[0])], p[3], p[1]
            )
        base_tree.hash_changed()

        files = sorted(list(tree.flatten().keys()))

        if tree != base_tree:
            logging.debug('validating rev: %s commit: %s' % (i, commit_id))
            logging.debug('validating files: %s   %s   INVALID TREE' % (
                len(files), len(base_files)))

            def so1(a):
                keys = [k['name'] for k in a['entries']]
                return b''.join(sorted(keys))

            tree_dirs = [d for d in tree.yield_swh_directories()]
            base_dirs = [d for d in base_tree.yield_swh_directories()]
            tree_dirs.sort(key=so1)
            base_dirs.sort(key=so1)

            logging.debug('Program will quit after your next Ctrl-D')
            code.interact(local=dict(globals(), **locals()))
            quit()
        else:
            logging.debug('v')

    def generate_all_commits(self, validate=True, frequency=1):
        i = 0
        start = time.time()
        for rev in self.get_revisions():
            logging.debug('.', end='')
            i += 1
            if i % 20 == 0:
                sys.stdout.flush()

        logging.debug('')
        logging.debug('\nFOUND: %s COMMITS' % i)
        logging.debug('ELAPSED: %s' % (time.time()-start))

    def runtest(self, hgdir, validate_blobs=False, validate_trees=False,
                frequency=1.0):
        """loader = HgLoaderMemoryStorage(0
           HgLoaderValidater(loader).runtest('/home/avi/SWH/mozilla-unified')

        """
        self.origin_id = 'test'

        dt = datetime.datetime.now(tz=datetime.timezone.utc)

        hgrepo = None
        if (hgdir.lower().startswith('http:')
                or hgdir.lower().startswith('https:')):
            hgrepo, hgdir = hgdir, hgrepo

        self.hgdir = hgdir

        try:
            logging.debug('preparing')
            self.loader.prepare(
                origin_url=hgrepo, visit_date=dt, directory=hgdir)

            self.file_node_to_hash = {}

            logging.debug('getting contents')
            cs = 0
            for c in self.loader.get_contents():
                cs += 1
                pass

            logging.debug('getting directories')
            ds = 0
            for d in self.loader.get_directories():
                ds += 1
                pass

            revs = 0
            logging.debug('getting revisions')
            for rev in self.loader.get_revisions():
                revs += 1
                pass

            logging.debug('getting releases')
            rels = 0
            for rel in self.loader.get_releases():
                rels += 1
                logging.debug(rel)

            self.visit = 'foo'
            snps = 0
            logging.debug('getting snapshot')
            o = self.loader.get_snapshot()
            logging.debug('Snapshot: %s' % o)
            if o:
                snps += 1

        finally:
            self.loader.cleanup()

        return cs, ds, revs, rels, snps


class BaseLoaderVerifierTest(BaseLoaderTest):
    def setUp(self, archive_name='the-sandbox.tgz', filename='the-sandbox'):
        super().setUp(archive_name=archive_name, filename=filename,
                      prefix_tmp_folder_name='swh.loader.mercurial.',
                      start_path=os.path.dirname(__file__))
        loader = HgLoaderMemoryStorage()
        self.validator = HgLoaderValidater(loader)


class LoaderVerifierTest1(BaseLoaderVerifierTest):
    def test_data(self):
        repo_path = urllib.parse.urlparse(self.repo_url).path
        cs, ds, revs, rels, snps = self.validator.runtest(
            repo_path,
            validate_blobs=True,
            validate_trees=True,
            frequency=0.001)

        self.assertEqual(cs, 2)
        self.assertEqual(ds, 3)
        self.assertEqual(revs, 58)
        self.assertEqual(rels, 0)
        self.assertEqual(snps, 1)


class LoaderVerifierTest2(BaseLoaderVerifierTest):
    def setUp(self, archive_name='hello.tgz', filename='hello'):
        super().setUp(archive_name=archive_name, filename=filename)

    def test_data(self):
        repo_path = urllib.parse.urlparse(self.repo_url).path
        cs, ds, revs, rels, snps = self.validator.runtest(
            repo_path,
            validate_blobs=True,
            validate_trees=True,
            frequency=0.001)

        self.assertEqual(cs, 3)
        self.assertEqual(ds, 3)
        self.assertEqual(rels, 1)
        self.assertEqual(revs, 3)
        self.assertEqual(snps, 1)