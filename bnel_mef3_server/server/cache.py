import collections
import threading
from concurrent import futures

class LRUCache:
    """A thread-safe Least Recently Used (LRU) cache.

    Attributes:
        capacity (int): Maximum number of items the cache can hold.
        cache (OrderedDict): Stores the cache items.
        lock (threading.Lock): Ensures thread safety.
    """
    def __init__(self, capacity: int):
        """Initializes the LRUCache.

        Args:
            capacity (int): The maximum number of items the cache can hold. Must be non-negative.

        Raises:
            ValueError: If capacity is negative.
        """
        if capacity < 0:
            raise ValueError("Capacity must be non-negative")
        self.capacity = capacity
        self.cache = collections.OrderedDict()
        self.lock = threading.Lock()

    def get(self, key):
        """Retrieves an item from the cache and marks it as recently used.

        Args:
            key: The key to retrieve from the cache.

        Returns:
            The value associated with the key, or None if not found.
        """
        with self.lock:
            if key not in self.cache:
                return None
            # Move the accessed item to the end of the dict
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key, value):
        """Adds an item to the cache, evicting the oldest if capacity is reached.

        Args:
            key: The key to add or update in the cache.
            value: The value to associate with the key.
        """
        with self.lock:
            if key in self.cache:
                self.cache[key] = value  # Update value first
                self.cache.move_to_end(key)
            else:
                self.cache[key] = value
            # If capacity is exceeded, remove the first item (the least recently used)
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

    def __contains__(self, key):
        """Checks if a key is in the cache.

        Args:
            key: The key to check for existence in the cache.

        Returns:
            bool: True if the key is in the cache, False otherwise.
        """
        with self.lock:
            return key in self.cache

