# onion_crawler/data_structures.py
import threading
import time
from collections import Counter

class SafeCounter:
    """Thread-safe counter with last update tracking."""
    def __init__(self, initial=0):
        self._value = initial
        self._lock = threading.Lock()
        self._last_update_time = time.time()

    def increment(self, amount=1):
        with self._lock:
            self._value += amount
            self._last_update_time = time.time()
            return self._value

    def get(self):
        with self._lock:
            return self._value

    def get_time_since_update(self):
        """Return seconds since last update."""
        with self._lock:
            # Ensure last_update_time is fetched within the lock
            last_update = self._last_update_time
        return time.time() - last_update

class SafeExtensionCounter:
    """Thread-safe counter for file extensions."""
    def __init__(self):
        self._extensions = Counter()
        self._lock = threading.Lock()

    def increment(self, extension: str):
        if not extension or extension == "no_extension":
            # Optionally track 'no_extension' separately if needed
            # extension = "no_extension"
            return # Or handle 'no_extension' if desired

        # Basic sanitization (optional)
        extension = extension.strip().lower()
        if not extension: return

        with self._lock:
            self._extensions[extension] += 1

    def get_all(self) -> dict:
        """Return a copy of the current extension counts."""
        with self._lock:
            return dict(self._extensions)

    def get_summary(self, top_n=10) -> dict:
        """Calculate and return the top N extensions with counts and percentages."""
        with self._lock:
            # Create a copy to work on outside the lock if calculations are complex
            extensions_copy = Counter(self._extensions)

        total = sum(extensions_copy.values())
        if total == 0:
            return {}

        # Get most common extensions
        most_common = extensions_copy.most_common(top_n)

        # Calculate percentages
        result = {}
        for ext, count in most_common:
            percentage = (count / total) * 100
            result[ext] = (count, round(percentage, 2))

        return result

class SafeSet:
    """Thread-safe set."""
    def __init__(self):
        self._items = set()
        self._lock = threading.Lock()

    def add(self, item) -> bool:
        """Add item to set. Return True if added, False if already present."""
        with self._lock:
            if item in self._items:
                return False
            self._items.add(item)
            return True

    def contains(self, item) -> bool:
        """Check if item is in the set."""
        with self._lock:
            return item in self._items

    def size(self) -> int:
        """Return the number of items in the set."""
        with self._lock:
            return len(self._items)

    def get_items(self) -> set:
         """Return a copy of the items in the set."""
         with self._lock:
             return set(self._items)
