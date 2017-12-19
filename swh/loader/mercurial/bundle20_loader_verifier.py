# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import code
import hglib
import os
import random
import sys
import time

from binascii import hexlify, unhexlify

from swh.model import hashutil

from .bundle20_loader import HgBundle20Loader
from .converters import PRIMARY_ALGO as ALGO
from .objects import SimpleTree


class HgLoaderValidater(HgBundle20Loader):
    def generate_all_blobs(self, validate=True, frequency=1):
        print('GENERATING BLOBS')
        i = 0
        start = time.time()
        u = set()
        for blob, node_info in self.br.yield_all_blobs():
            filename = node_info[0]
            header = node_info[2]
            i += 1

            bhash = hashutil.hash_data(blob, algorithms=set([ALGO]))[ALGO]
            self.file_node_to_hash[header['node']] = bhash

            u.update([bhash])

            if validate:
                if random.random() < frequency:
                    self.validate_blob(filename, header, blob)

            if i % 10000 == 0:
                print(i)

        print('')
        print('FOUND', i, 'BLOBS')
        print('FOUND', len(u), 'UNIQUE BLOBS')
        print('ELAPSED', time.time()-start)

    def validate_blob(self, filename, header, blob):
            if not self.hg:
                self.hg = hglib.open(self.hgdir)

            data = bytes(blob)

            filepath = os.path.join(self.hg.root(), bytes(filename))
            linknode = hexlify(header['linknode'])
            cat_contents = self.hg.cat([filepath], rev=linknode)

            if cat_contents != data:
                print('INTERNAL ERROR ERROR ERROR ERROR')
                print(filename)
                print(header)
                print('-----')
                print(cat_contents)
                print('---- vs ----')
                print(data)
                code.interact(local=dict(globals(), **locals()))
                quit()
            else:
                print('v', end='')

    def generate_all_trees(self, validate=True, frequency=1):
        print('GENERATING MANIFEST TREES')

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

            print('.', end='')
            if c % 20 == 0:
                sys.stdout.flush()
            if c % 10000 == 0:
                print(c)

        print('')
        print('FOUND', c, 'COMMIT MANIFESTS')
        print('FOUND', n, 'NEW DIRS')
        print('FOUND', len(u), 'UNIQUE DIRS')
        print('ELAPSED', time.time()-start)

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
            print('validating rev:', i, 'commit:', commit_id)
            print('validating files:', len(files), len(base_files))
            print('   INVALID TREE')

            def so1(a):
                keys = [k['name'] for k in a['entries']]
                return b''.join(sorted(keys))

            tree_dirs = [d for d in tree.yield_SWH_directories()]
            base_dirs = [d for d in base_tree.yield_SWH_directories()]
            tree_dirs.sort(key=so1)
            base_dirs.sort(key=so1)

            # for i in range(len(tree_dirs)):
            #     if tree_dirs[i] != base_dirs[i]:
            #         print(i)
            #         code.interact(local=dict(globals(), **locals()))

            print("Program will quit after your next Ctrl-D")
            code.interact(local=dict(globals(), **locals()))
            quit()
        else:
            print('v', end='')

    def generate_all_commits(self, validate=True, frequency=1):
        i = 0
        start = time.time()
        for rev in self.get_revisions():
            print('.', end='')
            i += 1
            if i % 20 == 0:
                sys.stdout.flush()

        print('')
        print('FOUND', i, 'COMMITS')
        print('ELAPSED', time.time()-start)

    def runtest(self, hgdir, validate_blobs=False, validate_trees=False,
                frequency=1.0):
        """
        HgLoaderValidater().runtest('/home/avi/SWH/mozilla-unified')
        """
        self.prepare('nil', hgdir, 'nil')

        self.file_node_to_hash = {}

        # blobs_pickle = 'blob_hashes.' + os.path.basename(hgdir) + '.pickle'
        # if os.path.exists(blobs_pickle):
        #     with open(blobs_pickle, 'rb') as pfile:
        #         self.file_node_to_hash = pickle.load(pfile)

        # if not self.file_node_to_hash:
        #     self.generate_all_blobs(validate=validate_blobs,
        #                             frequency=frequency)

        # with open(blobs_pickle, 'wb') as pfile:
        #     pickle.dump(self.file_node_to_hash, pfile)

        # self.generate_all_trees(validate=validate_trees, frequency=frequency)
        # self.generate_all_commits()
        for c in self.get_contents():
            pass
        for d in self.get_directories():
            pass
        for rev in self.get_revisions():
            pass
        for rel in self.get_releases():
            print(rel)
        self.visit = 'foo'
        for o in self.get_occurrences():
            print(o)


def main():
    if len(sys.argv) > 1:
        test_repo = sys.argv[1]
    else:
        print("Please pass in the path to an HG repository.")
        quit()

    while test_repo[-1] == '/':
        test_repo = test_repo[:-1]

    if len(sys.argv) > 2:
        validate_frequency = float(sys.argv[2])
    else:
        validate_frequency = 0.001

    HgLoaderValidater().runtest(test_repo, True, True, validate_frequency)


if __name__ == '__main__':
    main()
