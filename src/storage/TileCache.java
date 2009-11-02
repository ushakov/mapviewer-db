package storage;

import java.util.HashMap;
import java.util.Map;

public class TileCache<K, V> {
  private NoAllocLinkedList<K> used;
  private HashMap<K, V> cache;
  private int capacity;

  public TileCache(int capacity) {
    this.capacity = capacity;
    this.used = new NoAllocLinkedList<K>();
    this.cache = new HashMap<K, V>();
  }

  public void clear() {
    // Recycle bitmap memory.
    for (Map.Entry<K, V> entry: cache.entrySet()) {
      V value = entry.getValue();
      if (value != null) {
        freeItem(value);
      }
    }
    used.clear();
    cache.clear();
  }
  
  // Could be overridden to free resources (memory, files etc).
  protected void freeItem(V value) {
    // Default implementation does nothing.
  }
  
  public void put(K key, V value) {
    if (cache.containsKey(key)) {
      cache.put(key, value);
      markUse(key);
      return;
    }
    if (cache.size() == capacity) {
      K oldKey = used.removeFirst();
      V oldValue = cache.remove(oldKey);
      if (oldValue != null) {
        freeItem(oldValue);
      }
    }
    cache.put(key, value);
    markUse(key);
  }

  public final boolean hasKey(K key) {
    return cache.containsKey(key);
  }

  public V get(K key) {
    if (!cache.containsKey(key)) {
      return null;
    }
    markUse(key);
    return cache.get(key);
  }

  private void markUse(K key) {
    used.moveToBack(key);
  }

  public String debugKeys() {
    return cache.keySet().toString();
  }
}

