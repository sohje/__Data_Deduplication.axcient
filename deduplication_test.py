import os
import hashlib
import unittest

from deduplication import Deduplication, Blob


class DeduplicationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data = Deduplication()

    @classmethod
    def tearDownClass(cls):
        # cleanup meta and blob files
        os.remove(cls.data.metafile)
        for blob_id in set(cls.data.blobs.values()):
            os.remove('{}.blob'.format(blob_id))

    def test1_put_block(self):
        self.assertEqual(self.data.put_block('key', 'value'), 0)
        self.assertEqual(self.data.put_block('key1', 'value'), 0)
        self.assertEqual(self.data.put_block('key2', 'value'), 0)

        self.assertEqual(self.data.put_block('normalized_key', 'Data.'), 0)

    def test2_get_block_data(self):
        self.assertEqual(self.data.get_block('key'), b'value')
        self.assertEqual(self.data.get_block('key1'), b'value')
        self.assertEqual(self.data.get_block('key2'), b'value')

    def test3_get_missed_key(self):
        with self.assertRaises(ResourceWarning, msg='No data found.'):
            self.data.get_block('somekey')

    def test4_check_ids(self):
        self.assertIn('key', self.data.id_list)
        self.assertIn('key1', self.data.id_list)
        self.assertIn('key2', self.data.id_list)

    def test5_check_data_multi_id(self):
        data_hash = self.data.make_data_hash('value')
        blob = Blob(blob_id=self.data.blobs[data_hash]) # initialize Blob
        block = blob.blocks[data_hash]

        self.assertIn('key', block.id_list)
        self.assertIn('key1', block.id_list)
        self.assertIn('key2', block.id_list)

    def test6_deduplication(self):
        hash1 = self.data.id_list['key1']
        hash2 = self.data.id_list['key2']
        # same data hash
        self.assertEqual(hash1, hash2)
        # same blob id
        self.assertEqual(self.data.blobs[hash1], self.data.blobs[hash2])

    def test7_initialize_from_metafile(self):
        # initialize new Deduplication obj from metafile
        data = Deduplication(metafile=self.data.metafile)

        self.assertEqual(data.get_block('key'), b'value')
        self.assertEqual(data.get_block('key1'), b'value')
        self.assertEqual(data.get_block('key2'), b'value')

        self.assertEqual(data.get_block('normalized_key'), b'Data.')
        self.assertEqual(set(data.id_list.keys()), set(self.data.id_list.keys()))


if __name__ == "__main__":
    unittest.main()
