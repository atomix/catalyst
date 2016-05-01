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

import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.StringHasher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer registry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SerializerRegistry {
  private final Hasher hasher = new StringHasher();
  private final Map<Class<?>, TypeSerializerFactory> factories = new ConcurrentHashMap<>();
  private final Map<Class<?>, TypeSerializerFactory> abstractFactories = Collections.synchronizedMap(new LinkedHashMap<>(1024, 0.75f, true));
  private final Map<Class<?>, TypeSerializerFactory> defaultFactories = Collections.synchronizedMap(new LinkedHashMap<>(1024, 0.75f, true));
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
    resolve(new PrimitiveTypeResolver());
    resolve(new JdkTypeResolver());
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
    return hasher.hash32(type.getName());
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
   * @throws RegistrationException If the given {@code type} is already registered or if no default
   *         serializer could be found for the given type.
   */
  public synchronized SerializerRegistry register(Class<?> type, int id) {
    if (type == null)
      throw new NullPointerException("type cannot be null");

    // Search for a default serializer for the type.
    Class<?> baseType = findBaseType(type, defaultFactories);
    if (baseType == null) {
      throw new RegistrationException("no default serializer found for type: " + type);
    }
    return register(type, id, defaultFactories.get(baseType));
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
    return register(type, calculateTypeId(type), new DefaultTypeSerializerFactory(serializer));
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
    return register(type, calculateTypeId(type), factory);
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param id The serializable type ID.
   * @param serializer The serializer.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  @SuppressWarnings("rawtypes")
  public SerializerRegistry register(Class<?> type, int id, Class<? extends TypeSerializer> serializer) {
    return register(type, id, new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers the given class for serialization.
   *
   * @param type The serializable class.
   * @param factory The serializer factory.
   * @param id The serializable type ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} or {@code id} is already registered
   */
  public synchronized SerializerRegistry register(Class<?> type, int id, TypeSerializerFactory factory) {
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
   * Registers the given class as an abstract serializer for the given abstract type.
   *
   * @param abstractType The abstract type for which to register the serializer.
   * @param serializer The serializer class.
   * @return The serializer registry.
   */
  public SerializerRegistry registerAbstract(Class<?> abstractType, Class<? extends TypeSerializer> serializer) {
    return registerAbstract(abstractType, calculateTypeId(abstractType), new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers the given class as an abstract serializer for the given abstract type.
   *
   * @param abstractType The abstract type for which to register the serializer.
   * @param factory The serializer factory.
   * @return The serializer registry.
   */
  public SerializerRegistry registerAbstract(Class<?> abstractType, TypeSerializerFactory factory) {
    return registerAbstract(abstractType, calculateTypeId(abstractType), factory);
  }

  /**
   * Registers the given class as an abstract serializer for the given abstract type.
   *
   * @param abstractType The abstract type for which to register the serializer.
   * @param id The serializable type ID.
   * @param serializer The serializer class.
   * @return The serializer registry.
   */
  public SerializerRegistry registerAbstract(Class<?> abstractType, int id, Class<? extends TypeSerializer> serializer) {
    return registerAbstract(abstractType, id, new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers the given class as an abstract serializer for the given abstract type.
   *
   * @param abstractType The abstract type for which to register the serializer.
   * @param id The serializable type ID.
   * @param factory The serializer factory.
   * @return The serializer registry.
   */
  public SerializerRegistry registerAbstract(Class<?> abstractType, int id, TypeSerializerFactory factory) {
    abstractFactories.put(abstractType, factory);
    types.put(id, abstractType);
    ids.put(abstractType, id);
    return this;
  }

  /**
   * Registers the given class as a default serializer for the given base type.
   *
   * @param baseType The base type for which to register the serializer.
   * @param serializer The serializer class.
   * @return The serializer registry.
   */
  public SerializerRegistry registerDefault(Class<?> baseType, Class<? extends TypeSerializer> serializer) {
    return registerDefault(baseType, new DefaultTypeSerializerFactory(serializer));
  }

  /**
   * Registers the given factory as a default serializer factory for the given base type.
   *
   * @param baseType The base type for which to register the serializer.
   * @param factory The serializer factory.
   * @return The serializer registry.
   */
  public synchronized SerializerRegistry registerDefault(Class<?> baseType, TypeSerializerFactory factory) {
    defaultFactories.put(baseType, factory);
    return this;
  }

  /**
   * Finds a serializable base type for the given type in the given factories map.
   */
  private Class<?> findBaseType(Class<?> type, Map<Class<?>, TypeSerializerFactory> factories) {
    if (factories.containsKey(type))
      return type;

    List<Map.Entry<Class<?>, TypeSerializerFactory>> orderedFactories = new ArrayList<>(factories.entrySet());
    Collections.reverse(orderedFactories);

    Optional<Map.Entry<Class<?>, TypeSerializerFactory>> optional = orderedFactories.stream()
      .filter(e -> e.getKey().isAssignableFrom(type))
      .findFirst();
    return optional.isPresent() ? optional.get().getKey() : null;
  }

  /**
   * Looks up the serializer for the given class else {@code null} if no serializer is registered for the {@code type}.
   *
   * @param type The serializable class.
   * @return The serializer for the given class.
   */
  synchronized TypeSerializerFactory factory(Class<?> type) {
    TypeSerializerFactory factory = factories.get(type);
    if (factory != null) {
      return factory;
    }

    Class<?> baseType;

    // If no factory was found, determine if an abstract serializer can be used.
    baseType = findBaseType(type, abstractFactories);
    if (baseType != null) {
      return abstractFactories.get(baseType);
    }

    // If no factory was found, determine if a default serializer can be used.
    baseType = findBaseType(type, defaultFactories);
    if (baseType != null) {
      return defaultFactories.get(baseType);
    }
    return null;
  }

  /**
   * Looks up the serializable type ID for the given type.
   */
  synchronized int id(Class<?> type) {
    Integer id = ids.get(type);
    if (id != null)
      return id;

    // If no ID was found for the given type, determine whether the type is an abstract type.
    Class<?> baseType = findBaseType(type, abstractFactories);
    if (baseType != null) {
      id = ids.get(baseType);
      if (id != null) {
        return id;
      }
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
