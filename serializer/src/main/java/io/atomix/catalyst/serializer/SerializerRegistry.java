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
import io.atomix.catalyst.serializer.util.ExternalizableSerializer;
import io.atomix.catalyst.util.Hash;

import java.io.Externalizable;
import java.util.*;

/**
 * Serializer registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SerializerRegistry implements Cloneable {
  private static final SerializableTypeResolver PRIMITIVE_RESOLVER = new PrimitiveTypeResolver();
  private static final SerializableTypeResolver JDK_RESOLVER = new JdkTypeResolver();
  private Map<Class<?>, TypeSerializerFactory> factories;
  private Map<Class<?>, Integer> ids;
  private Map<Integer, Class<?>> types;

  @SuppressWarnings("unchecked")
  public SerializerRegistry() {
    this(Collections.EMPTY_LIST);
  }

  public SerializerRegistry(SerializableTypeResolver... resolvers) {
    this(Arrays.asList(resolvers));
  }

  public SerializerRegistry(Collection<SerializableTypeResolver> resolvers) {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>());

    PRIMITIVE_RESOLVER.resolve(this);
    JDK_RESOLVER.resolve(this);
    resolve(resolvers);
  }

  private SerializerRegistry(Map<Class<?>, TypeSerializerFactory> factories, Map<Class<?>, Integer> ids, Map<Integer, Class<?>> types) {
    this.factories = factories;
    this.ids = ids;
    this.types = types;
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
   * Copies the serializer.
   */
  SerializerRegistry copy() {
    return new SerializerRegistry(new HashMap<>(factories), new HashMap<>(ids), new HashMap<>(types));
  }

  /**
   * Returns the type ID for the given class.
   */
  private int findTypeId(Class<?> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");

    SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);

    if (serializeWith != null) {
      int id = serializeWith.id();
      if (id == -1) {
        return Hash.hash32(type.getName());
      }
      return id;
    } else {
      return Hash.hash32(type.getName());
    }
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
    return register(type, findTypeId(type));
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

    SerializeWith serializeWith = type.getAnnotation(SerializeWith.class);
    if (serializeWith != null && serializeWith.serializer() != null) {
      return register(type, new DefaultTypeSerializerFactory(serializeWith.serializer()), id);
    } else if (CatalystSerializable.class.isAssignableFrom(type)) {
      return register(type, new DefaultTypeSerializerFactory(CatalystSerializableSerializer.class), id);
    } else if (Externalizable.class.isAssignableFrom(type)) {
      return register(type, new DefaultTypeSerializerFactory(ExternalizableSerializer.class), id);
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
    return register(type, new DefaultTypeSerializerFactory(serializer), findTypeId(type));
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
    return register(type, factory, findTypeId(type));
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
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory, int id) {
    if (type == null)
      throw new NullPointerException("type cannot be null");

    if (ids.containsKey(type)) {
      if (ids.get(type) != id) {
        throw new RegistrationException("type registered with a different id: " + type);
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
  public TypeSerializerFactory lookup(Class<?> type) {
    TypeSerializerFactory factory = factories.get(type);
    if (factory == null) {
      for (Map.Entry<Class<?>, TypeSerializerFactory> entry : factories.entrySet()) {
        if (entry.getKey().isAssignableFrom(type)) {
          factory = entry.getValue();
          break;
        }
      }

      if (factory != null) {
        factories.put(type, factory);
      } else {
        factories.put(type, null);
      }
    }
    return factory;
  }

  /**
   * Returns a map of registered ids and their IDs.
   */
  Map<Class<?>, Integer> ids() {
    return ids;
  }

  /**
   * Returns a map of serialization IDs and registered ids.
   */
  Map<Integer, Class<?>> types() {
    return types;
  }

  @Override
  public final SerializerRegistry clone() {
    try {
      SerializerRegistry clone = (SerializerRegistry) super.clone();
      clone.ids = new HashMap<>(ids);
      clone.factories = new HashMap<>(factories);
      clone.types = new HashMap<>(types);
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

}
