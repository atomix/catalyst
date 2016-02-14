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
package io.atomix.catalyst.serializer;

import io.atomix.catalyst.buffer.BufferAllocator;
import io.atomix.catalyst.buffer.UnpooledHeapAllocator;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.PropertiesReader;

import java.util.Map;
import java.util.Properties;

/**
 * Serializer properties.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class SerializerProperties {
  private static final String WHITELIST = "whitelist";
  private static final String ALLOCATOR = "allocator";
  private static final String TYPES = "types";
  private static final String SERIALIZERS = "serializers";
  private static final String ABSTRACT_SERIALIZERS = "abstractSerializers";
  private static final String DEFAULT_SERIALIZERS = "defaultSerializers";

  private final PropertiesReader reader;

  public SerializerProperties(Properties properties) {
    this.reader = new PropertiesReader(properties);
  }

  /**
   * Indicates whether to require whitelisting.
   *
   * @return Whether to require whitelisting.
   */
  boolean whitelist() {
    return reader.getBoolean(WHITELIST, true);
  }

  /**
   * Returns the serializer buffer allocator.
   *
   * @return The serializer buffer allocator.
   */
  BufferAllocator allocator() {
    Class<?> allocator = reader.getClass(ALLOCATOR, UnpooledHeapAllocator.class);
    try {
      return (BufferAllocator) allocator.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConfigurationException(e);
    } catch (ClassCastException e) {
      throw new ConfigurationException("invalid allocator class: " + allocator.getName());
    }
  }

  /**
   * Returns a map of serializable types defined by the properties.
   *
   * @return A map of serializable types defined by the properties.
   */
  Map<Integer, Class<?>> types() {
    return reader.getMap(TYPES, this::idToInt, this::typeToClass);
  }

  /**
   * Returns a map of type serializers.
   *
   * @return A map of type serializers.
   */
  Map<Integer, Class<?>> serializers() {
    return reader.getMap(SERIALIZERS, this::idToInt, this::serializerToClass);
  }

  /**
   * Returns a map of abstract serializers.
   *
   * @return A map of abstract serializers.
   */
  Map<Integer, Class<?>> abstractSerializers() {
    return reader.getMap(ABSTRACT_SERIALIZERS, this::idToInt, this::serializerToClass);
  }

  /**
   * Returns a map of default serializers.
   *
   * @return A map of default serializers.
   */
  Map<Integer, Class<?>> defaultSerializers() {
    return reader.getMap(DEFAULT_SERIALIZERS, this::idToInt, this::serializerToClass);
  }

  /**
   * Converts a string to an integer.
   */
  private int idToInt(String value) {
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      throw new ConfigurationException("invalid type ID: " + value);
    }
  }

  /**
   * Converts a string to a class.
   */
  private Class<?> serializerToClass(String value) {
    try {
      return Thread.currentThread().getContextClassLoader().loadClass(value);
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("unknown serializable type: " + value);
    }
  }

  /**
   * Converts a string to a class.
   */
  private Class<?> typeToClass(String value) {
    try {
      return Thread.currentThread().getContextClassLoader().loadClass(value);
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("unknown serializable type: " + value);
    }
  }

}
