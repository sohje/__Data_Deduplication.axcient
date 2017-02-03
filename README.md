# Data Deduplication
#### Python 3 only

#### Example
```python
$> python
>>> from deduplication import Deduplication
>>> data = Deduplication(block_size=8, blob_size=2)
>>> data.put_block('key', 'value')
0
>>> data.put_block('key1', 'value')
0
>>> data.put_block('key2', 'somedata')
0
>>> data.get_block('key2')
b'somedata'
>>> data.get_block('key')
b'value'
>>> data.get_block('key1')
b'value'
```

#### tests
```shell
python deduplication_test.py
```
