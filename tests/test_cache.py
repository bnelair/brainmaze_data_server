import pytest
import threading
from bnel_mef3_server.server.cache import LRUCache

def test_put_and_get():
    cache = LRUCache(2)
    cache.put('a', 1)
    assert cache.get('a') == 1
    assert cache.get('b') is None

def test_eviction_policy():
    cache = LRUCache(2)
    cache.put('a', 1)
    cache.put('b', 2)
    cache.put('c', 3)  # 'a' should be evicted
    assert cache.get('a') is None
    assert cache.get('b') == 2
    assert cache.get('c') == 3

def test_recently_used_update():
    cache = LRUCache(2)
    cache.put('a', 1)
    cache.put('b', 2)
    # Access 'a' to make it recently used
    assert cache.get('a') == 1
    cache.put('c', 3)  # 'b' should be evicted
    assert cache.get('b') is None
    assert cache.get('a') == 1
    assert cache.get('c') == 3

def test_update_existing_key():
    cache = LRUCache(2)
    cache.put('a', 1)
    cache.put('a', 2)
    assert cache.get('a') == 2
    cache.put('b', 3)
    cache.put('c', 4)
    # 'a' was not accessed after 'b' was added, so 'a' should be evicted
    assert cache.get('a') is None
    assert cache.get('b') == 3
    assert cache.get('c') == 4

def test_contains():
    cache = LRUCache(2)
    cache.put('a', 1)
    assert 'a' in cache
    assert 'b' not in cache
    cache.put('b', 2)
    cache.put('c', 3)
    assert 'a' not in cache  # 'a' should be evicted

def test_capacity_one():
    cache = LRUCache(1)
    cache.put('a', 1)
    assert cache.get('a') == 1
    cache.put('b', 2)
    assert cache.get('a') is None
    assert cache.get('b') == 2

def test_none_key_and_value():
    cache = LRUCache(2)
    cache.put(None, None)
    assert cache.get(None) is None  # None as key
    cache.put('a', None)
    assert cache.get('a') is None

def test_thread_safety():
    cache = LRUCache(5)
    def writer():
        for i in range(100):
            cache.put(f'k{i%5}', i)
    def reader():
        for i in range(100):
            _ = cache.get(f'k{i%5}')
    threads = [threading.Thread(target=writer) for _ in range(5)] + \
              [threading.Thread(target=reader) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # After all, only 5 keys should remain
    keys = [f'k{i}' for i in range(5)]
    for k in keys:
        assert k in cache

def test_zero_capacity():
    cache = LRUCache(0)
    cache.put('a', 1)
    assert cache.get('a') is None
    assert 'a' not in cache

def test_negative_capacity():
    with pytest.raises(ValueError):
        LRUCache(-1)
