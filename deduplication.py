"""
Data Deduplication proto
"""

import pickle
import hashlib
import sys
from time import time


if sys.version_info[0] < 3:
    raise RuntimeError("Must be using Python 3")

def get_ts():
    return int(time() * 1000)


class Block:
    """Main block class.

    Minimal unit to store data.
    """
    def __init__(self, size, block_id, data):
        self.size = size
        self.data = bytes(str(data), 'utf-8')[:self.size]
        self.hash = hashlib.sha1(self.data).hexdigest()
        self.id_list = {block_id}

    def add_id(self, block_id):
        self.id_list.add(block_id)
        return 0

    def remove_id(self, block_id):
        try:
            self.id_list.remove(block_id)
            return 0
        except KeyError:
            return 1


class Blob:
    """Main blob class.

    Control data blocks - store, remove, etc.
    Also, blob can contain additional info - self.meta dict.
    """
    def __init__(self, size=8, blob_id=None):
        ts = get_ts()

        self.max_size = size
        self.blocks = {}
        self.id = self._gen_id()
        # extend meta if needed
        self.meta = {'modified': ts, 'created': ts}
        if blob_id:
            self._read_blob(blob_id)

    def _read_blob(self, blob_id):
        """Read blob file, deserialize date, update internal dict.

        :return: None
        """
        with open('{}.blob'.format(blob_id), 'rb') as f:
            self.__dict__.update(pickle.load(f))

    def update_blob(self):
        with open('{}.blob'.format(self.id), 'wb') as f:
            pickle.dump(self.__dict__, f)

    def add_block(self, block):
        if self.size == self.max_size or not isinstance(block, Block):
            # raise AssertionError()
            # raise ResourceWarning('BLOB LIMIT REACHED.')
            return 1

        self.blocks[block.hash] = block
        self.meta['modified'] = get_ts()
        self.update_blob()
        return 0

    def remove_block_by_hash(self, block_hash):
        self.blocks.pop(block_hash, None)

    def remove_block_by_id(self, block_id):
        pass

    @staticmethod
    def _gen_id():
        return (get_ts() - 514941780) << 22

    @property
    def size(self):
        return len(self.blocks)


class Deduplication:
    """ Main data deduplication class.

    Control system storage - organize blobs, put data in available blob, retrieve data, etc.
    Can be initialized from existing metafile.

    self.id_list {<block_id>: <data_hash>} speedup lookups by block_id
    self.blobs {<data_hash>: (:obj:`Blob.id`)}
    """
    def __init__(self, block_size=8, blob_size=8, metafile=None):
        self.block_size = block_size
        self.blob_size = blob_size
        self.id_list = {}
        self.blobs = {}
        self.metafile = 'metafile-{}'.format(int(time()))

        if metafile:
            self.metafile = metafile
            self.__init_from_metafile()

    def __init_from_metafile(self):
        with open(self.metafile, 'rb') as f:
            self.__dict__.update(pickle.load(f))

    def _find_available_blob(self):
        for blob_id in set(self.blobs.values()):
            # initialize Blob object with blob_id
            blob = Blob(blob_id=blob_id)
            if blob.size < self.blob_size:
                return blob

        return Blob(self.blob_size)

    def _update_metafile(self):
        """Overwrite metafile with updated data.

        :return: bool: True if successful, False otherwise.
        """
        with open(self.metafile, 'wb') as f:
            pickle.dump(self.__dict__, f)


    @staticmethod
    def make_data_hash(data):
        return hashlib.sha1(bytes(str(data), 'utf-8')).hexdigest()

    def put_block(self, block_id, block_data):
        if block_id in self.id_list:
            # should we overwrite existing block_data?
            return 0

        data_hash = self.make_data_hash(block_data)
        # check if same block_data already exists
        if data_hash in self.blobs:
            self.id_list[block_id] = data_hash
            blob = Blob(blob_id=self.blobs[data_hash])
            blob.blocks[data_hash].add_id(block_id)
            blob.update_blob() # simple workaround, flush blob to file with updated Block.id_list
        else:
            # looks like we have to find available Blob to store provided data
            self.id_list[block_id] = data_hash
            blob = self._find_available_blob()
            blob.add_block(Block(self.block_size, block_id, block_data))
            self.blobs[data_hash] = blob.id

        self._update_metafile()
        return 0

    def get_block(self, block_id):
        if block_id not in self.id_list:
            raise ResourceWarning('No data found.')

        data_hash = self.id_list[block_id]
        blob = Blob(blob_id=self.blobs[data_hash])
        return blob.blocks[data_hash].data
