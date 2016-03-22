/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.catalyst.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A fixed capacity map that automatically makes room for a new entry when the map is full by evicting the least
 * recently accessed entry.
 * <p>
 * This implementation is not synchronized. Concurrent access to a LRUCache from multiple threads
 * must be synchronized externally.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
@SuppressWarnings("serial")
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
  private final int maxSize;

  public LRUCache(int maxSize) {
    super(16, 0.75f, true);
    this.maxSize = Assert.arg(maxSize, maxSize > 0, "maxSize must be positive");
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
    return size() > maxSize;
  }
}