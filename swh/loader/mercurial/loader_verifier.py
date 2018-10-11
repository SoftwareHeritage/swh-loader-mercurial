# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import code
import datetime
import hglib
import logging
import os
import random
import sys
import time

from binascii import hexlify, unhexlify

from swh.model.hashutil import MultiHash

from .loader import HgBundle20Loader
from .converters import PRIMARY_ALGO as ALGO
from .objects import SimpleTree


class HgLoaderValidater(HgBundle20Loader):
    def generate_all_blobs(self, validate=True, frequency=1):
        logging.debug('GENERATING BLOBS')
        i = 0
        start = time.time()
        u = set()
        for blob, node_info in self.br.yield_all_blobs():
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

        for header, tree, new_dirs in self.load_directories():
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

            # for i in range(len(tree_dirs)):
            #     if tree_dirs[i] != base_dirs[i]:
            #         logging.debug(i)
            #         code.interact(local=dict(globals(), **locals()))

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
                frequency=1.0, test_iterative=False):
        """HgLoaderValidater().runtest('/home/avi/SWH/mozilla-unified')

        """
        self.origin_id = 'test'

        dt = datetime.datetime.now(tz=datetime.timezone.utc)
        if test_iterative:
            dt = dt - datetime.timedelta(10)

        hgrepo = None
        if (hgdir.lower().startswith('http:')
                or hgdir.lower().startswith('https:')):
            hgrepo, hgdir = hgdir, hgrepo

        self.hgdir = hgdir

        try:
            logging.debug('preparing')
            self.prepare(origin_url=hgrepo, visit_date=dt, directory=hgdir)

            self.file_node_to_hash = {}

        # self.generate_all_blobs(validate=validate_blobs,
        #                        frequency=frequency)

        # self.generate_all_trees(validate=validate_trees, frequency=frequency)
        # self.generate_all_commits()
            logging.debug('getting contents')
            cs = 0
            for c in self.get_contents():
                cs += 1
                pass

            logging.debug('getting directories')
            ds = 0
            for d in self.get_directories():
                ds += 1
                pass

            revs = 0
            logging.debug('getting revisions')
            for rev in self.get_revisions():
                revs += 1
                pass

            logging.debug('getting releases')
            rels = 0
            for rel in self.get_releases():
                rels += 1
                logging.debug(rel)

            self.visit = 'foo'
            logging.debug('getting snapshot')
            o = self.get_snapshot()
            logging.debug('Snapshot: %s' % o)

        finally:
            self.cleanup()

        logging.info('final count: cs %s ds %s revs %s rels %s' % (
            cs, ds, revs, rels))


@click.command()
@click.option('--verbose', is_flag=True, default=False)
@click.option('--validate-frequency', default=0.001, type=click.FLOAT)
@click.option('--test-iterative', default=False, type=click.BOOL)
@click.argument('repository-url', required=1)
def main(verbose, validate_frequency, test_iterative, repository_url):
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    while repository_url[-1] == '/':
        repository_url = repository_url[:-1]

    HgLoaderValidater().runtest(
        repository_url,
        validate_blobs=True, validate_trees=True,
        frequency=validate_frequency,
        test_iterative=test_iterative)


if __name__ == '__main__':
    main()
