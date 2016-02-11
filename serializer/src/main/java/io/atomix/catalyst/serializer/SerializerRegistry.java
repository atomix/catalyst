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
 * limitations under the License.
 */
package io.atomix.catalyst.serializer;

import io.atomix.catalyst.CatalystException;
import io.atomix.catalyst.serializer.util.CatalystSerializableSerializer;
import io.atomix.catalyst.serializer.util.ExternalizableSerializer;
import io.atomix.catalyst.serializer.util.JavaSerializableSerializer;
import io.atomix.catalyst.util.Hash;

import java.io.Externalizable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SerializerRegistry {
  private static final SerializableTypeResolver PRIMITIVE_RESOLVER = new PrimitiveTypeResolver();
  private static final SerializableTypeResolver JDK_RESOLVER = new JdkTypeResolver();
  private final Map<Class<?>, TypeSerializerFactory> factories = new ConcurrentHashMap<>();
  private final Map<Class<?>, Integer> ids = new ConcurrentHashMap<>();
  private final Map<Integer, Class<?>> types = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  public SerializerRegistry() {
    this(Collections.EMPTY_LIST);
  }

  public SerializerRegistry(SerializableTypeResolver... resolvers) {
    this(Arrays.asList(resolvers));
  }

  public SerializerRegistry(Collection<SerializableTypeResolver> resolvers) {
    PRIMITIVE_RESOLVER.resolve(this);
    JDK_RESOLVER.resolve(this);
    resolve(resolvers);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer registry instance.
   */
  @SuppressWarnings("unchecked")
  public SerializerRegistry resolve(SerializableTypeResolver... resolvers) {
    return resolve(resolvers != null ? Arrays.asList(resolvers) : Collections.EMPTY_LIST);
  }

  /**
   * Resolves serializable types with the given resolver.
   * <p>
   * This allows users to modify the serializable types registered to an existing {@link Serializer} instance. Types resolved
   * by the provided resolver(s) will be added to existing types resolved by any type resolvers provided to this object's
   * constructor or by previous calls to this method.
   *
   * @param resolvers The resolvers with which to resolve serializable types.
   * @return The serializer registry instance.
   */
  public SerializerRegistry resolve(Collection<SerializableTypeResolver> resolvers) {
    if (resolvers != null) {
      for (SerializableTypeResolver resolver : resolvers) {
        resolver.resolve(this);
      }
    }
    return this;
  }

  /**
   * Returns the type ID for the given class.
   */
  private int calculateTypeId(Class<?> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    return Hash.hash32(type.getName());
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The type class.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    return register(type, calculateTypeId(type));
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, int id) {
    if (type == null)
      throw new NullPointerException("type cannot be null");

    if (CatalystSerializable.class.isAssignableFrom(type)) {
      return register(type, new DefaultTypeSerializerFactory(CatalystSerializableSerializer.class), id);
    } else if (Externalizable.class.isAssignableFrom(type)) {
      return register(type, new DefaultTypeSerializerFactory(ExternalizableSerializer.class), id);
    } else if (Serializable.class.isAssignableFrom(type)) {
      return register(type, new DefaultTypeSerializerFactory(JavaSerializableSerializer.class), id);
    } else {
      throw new CatalystException("failed to register serializable type: " + type);
    }
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class.
   * @param serializer The serializer.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  @SuppressWarnings("rawtypes")
  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer) {
    return register(type, new DefaultTypeSerializerFactory(serializer), calculateTypeId(type));
  }

  /**
   * Registers a serializer for the given class.
   *
   * @param type The serializable class.
   * @param factory The serializer factory.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory) {
    return register(type, factory, calculateTypeId(type));
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param serializer The serializer.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  @SuppressWarnings("rawtypes")
  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer, int id) {
    return register(type, new DefaultTypeSerializerFactory(serializer), id);
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param factory The serializer factory.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} or {@code id} is already registered
   */
  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory, int id) {
    if (type == null)
      throw new NullPointerException("type cannot be null");

    // If the type ID has already been registered, throw an exception.
    if (types.containsKey(id) && types.get(id) != type) {
      throw new RegistrationException("serializable type ID already registered: " + id);
    }

    // If the type has already been registered, throw an exception if the IDs don't match.
    if (ids.containsKey(type)) {
      if (ids.get(type) != id) {
        throw new RegistrationException("type registered with a different ID: " + type);
      }
      return this;
    }

    factories.put(type, factory);
    types.put(id, type);
    ids.put(type, id);
    return this;
  }

  /**
   * Looks up the serializer for the given class else {@code null} if no serializer is registered for the {@code type}.
   *
   * @param type The serializable class.
   * @return The serializer for the given class.
   */
  TypeSerializerFactory factory(Class<?> type) {
    TypeSerializerFactory factory = factories.get(type);
    if (factory == null) {
      for (Map.Entry<Class<?>, TypeSerializerFactory> entry : factories.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          factory = entry.getValue();
          break;
        }
      }

      // If no factory was found, determine if a Java serializable factory can be used.
      if (factory == null) {
        if (CatalystSerializable.class.isAssignableFrom(type)) {
          factory = new DefaultTypeSerializerFactory(CatalystSerializableSerializer.class);
        } else if (Externalizable.class.isAssignableFrom(type)) {
          factory = new DefaultTypeSerializerFactory(ExternalizableSerializer.class);
        } else if (Serializable.class.isAssignableFrom(type)) {
          factory = new DefaultTypeSerializerFactory(JavaSerializableSerializer.class);
        }
      }

      factories.put(type, factory);
    }
    return factory;
  }

  /**
   * Looks up the serializable type ID for the given type.
   */
  int id(Class<?> type) {
    Integer id = ids.get(type);
    if (id != null)
      return id;

    for (Map.Entry<Class<?>, Integer> entry : ids.entrySet()) {
      if (entry.getKey().isAssignableFrom(type)) {
        id = entry.getValue();
        break;
      }
    }

    if (id != null) {
      ids.put(type, id);
      return id;
    }
    return 0;
  }

  /**
   * Returns the type for the given ID.
   *
   * @param id The ID for which to return the type.
   * @return The type for the given ID.
   */
  Class<?> type(int id) {
    return types.get(id);
  }

}
