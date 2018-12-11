# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from unittest.mock import patch

from swh.loader.mercurial.tasks import LoadMercurial, LoadArchiveMercurial


class TestTasks(unittest.TestCase):
    def test_check_task_name(self):
        task = LoadMercurial()
        self.assertEqual(task.task_queue, 'swh_loader_mercurial')

    @patch('swh.loader.mercurial.loader.HgBundle20Loader.load')
    def test_task(self, mock_loader):
        mock_loader.return_value = {'status': 'eventful'}
        task = LoadMercurial()

        # given
        actual_result = task.run_task(
            origin_url='origin_url', visit_date='now', directory='/some/repo')

        self.assertEqual(actual_result, {'status': 'eventful'})

        mock_loader.assert_called_once_with(
            origin_url='origin_url', visit_date='now', directory='/some/repo')


class TestTasks2(unittest.TestCase):
    def test_check_task_name(self):
        task = LoadArchiveMercurial()
        self.assertEqual(task.task_queue, 'swh_loader_mercurial_archive')

    @patch('swh.loader.mercurial.loader.HgArchiveBundle20Loader.load')
    def test_task(self, mock_loader):
        mock_loader.return_value = {'status': 'uneventful'}
        task = LoadArchiveMercurial()

        # given
        actual_result = task.run_task(
            origin_url='another_url',
            archive_path='/some/tar.tgz',
            visit_date='now')

        self.assertEqual(actual_result, {'status': 'uneventful'})

        mock_loader.assert_called_once_with(
            origin_url='another_url',
            archive_path='/some/tar.tgz',
            visit_date='now')
