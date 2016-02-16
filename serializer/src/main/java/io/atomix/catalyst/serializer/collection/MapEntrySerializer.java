/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.catalyst.serializer.collection;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.util.Assert;

import java.util.Map;

/**
 * Map entry serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapEntrySerializer implements TypeSerializer<Map.Entry<?, ?>> {

  @Override
  @SuppressWarnings("unchecked")
  public void write(Map.Entry<?, ?> object, BufferOutput buffer, Serializer serializer) {
    serializer.writeObject(object.getKey(), buffer);
    serializer.writeObject(object.getValue(), buffer);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map.Entry<?, ?> read(Class<Map.Entry<?, ?>> type, BufferInput buffer, Serializer serializer) {
    Object key = serializer.readObject(buffer);
    Object value = serializer.readObject(buffer);
    return new MapEntry<>(key, value);
  }

  /**
   * Map entry.
   */
  private static class MapEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    private MapEntry(K key, V value) {
      this.key = Assert.notNull(key, "key");
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      V oldValue = this.value;
      this.value = value;
      return oldValue;
    }
  }

}
