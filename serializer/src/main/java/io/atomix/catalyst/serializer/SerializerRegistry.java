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
import io.atomix.catalyst.serializer.resolver.SerializableTypeResolver;
import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.StringHasher;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
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
   * Returns the scope for the given type.
   */
  private TypeSerializer.Scope scopeFor(Class<?> type) {
    if (isTypeSerializerClass(type)) {
      SerializerInfo info = findSerializerInfo(type);
      if (info != null && info.scope() != TypeSerializer.Scope.NONE) {
        return info.scope();
      }

      if (ConcreteSerializer.class.isAssignableFrom(type)) {
        return TypeSerializer.Scope.CONCRETE;
      } else if (AbstractSerializer.class.isAssignableFrom(type)) {
        return TypeSerializer.Scope.ABSTRACT;
      } else if (DefaultSerializer.class.isAssignableFrom(type)) {
        return TypeSerializer.Scope.DEFAULT;
      }
      return TypeSerializer.Scope.CONCRETE;
    } else {
      SerializableTypeInfo info = findSerializableTypeInfo(type);
      if (info != null && info.serializer() != SerializableTypeInfo.None.class) {
        Class<?> serializer = info.serializer();
        SerializerInfo serializerInfo = findSerializerInfo(serializer);
        if (serializerInfo != null && serializerInfo.scope() != TypeSerializer.Scope.NONE) {
          return serializerInfo.scope();
        }

        if (ConcreteSerializer.class.isAssignableFrom(serializer)) {
          return TypeSerializer.Scope.CONCRETE;
        } else if (AbstractSerializer.class.isAssignableFrom(serializer)) {
          return TypeSerializer.Scope.ABSTRACT;
        } else if (DefaultSerializer.class.isAssignableFrom(serializer)) {
          return TypeSerializer.Scope.DEFAULT;
        }
      }

      if (Modifier.isAbstract(type.getModifiers()) || Modifier.isInterface(type.getModifiers())) {
        return TypeSerializer.Scope.ABSTRACT;
      }
      return TypeSerializer.Scope.CONCRETE;
    }
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The type class.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type) {
    return register(type, calculateTypeId(type), findTypeSerializerFactory(type), scopeFor(type));
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The serializable class.
   * @param typeId The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered or if no default
   *         serializer could be found for the given type.
   */
  public SerializerRegistry register(Class<?> type, int typeId) {
    return register(type, typeId, findTypeSerializerFactory(type), scopeFor(type));
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The serializable class.
   * @param serializer The serializer.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer) {
    return register(type, serializer, scopeFor(serializer));
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The serializable class.
   * @param typeId The serializable type ID.
   * @param serializer The serializer.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry register(Class<?> type, int typeId, Class<? extends TypeSerializer> serializer) {
    return register(type, typeId, serializer, scopeFor(serializer));
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The serializable class.
   * @param factory The serializer factory.
   * @param typeId The serializable type ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} or {@code id} is already registered
   */
  public SerializerRegistry register(Class<?> type, int typeId, TypeSerializerFactory factory) {
    return registerConcrete(type, typeId, factory);
  }

  public SerializerRegistry register(Class<?> type, Class<? extends TypeSerializer> serializer, TypeSerializer.Scope scope) {
    return register(type, new DefaultTypeSerializerFactory(serializer), scope);
  }

  public SerializerRegistry register(Class<?> type, TypeSerializerFactory factory, TypeSerializer.Scope scope) {
    if (scope == null)
      throw new NullPointerException("scope cannot be null");

    switch (scope) {
      case DEFAULT:
        return registerDefault(type, factory);
      default:
        return register(type, calculateTypeId(type), factory, scope);
    }
  }

  public SerializerRegistry register(Class<?> type, int typeId, Class<? extends TypeSerializer> serializer, TypeSerializer.Scope scope) {
    return register(type, typeId, new DefaultTypeSerializerFactory(serializer), scope);
  }

  public SerializerRegistry register(Class<?> type, int typeId, TypeSerializerFactory factory, TypeSerializer.Scope scope) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    if (scope == null)
      throw new NullPointerException("scope cannot be null");

    switch (scope) {
      case CONCRETE:
        return registerConcrete(type, typeId, factory);
      case ABSTRACT:
        return registerAbstract(type, typeId, factory);
      default:
        throw new IllegalArgumentException("cannot register DEFAULT serializer with type ID");
    }
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The type class.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered
   */
  public SerializerRegistry registerConcrete(Class<?> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");

    if (isTypeSerializerClass(type)) {
      return registerConcreteSerializer(type);
    } else {
      return registerConcrete(type, calculateTypeId(type));
    }
  }

  /**
   * Registers a concrete serializer class.
   */
  private SerializerRegistry registerConcreteSerializer(Class<?> type) {
    validateConcreteSerializer(type);
    SerializerInfo info = findSerializerInfo(type);
    TypeSerializerFactory factory = findTypeSerializerFactory(type, info);
    for (SerializableTypeReference reference : info.value()) {
      if (reference.id() != 0) {
        registerConcrete(reference.value(), reference.id(), factory);
      } else {
        registerConcrete(reference.value(), calculateTypeId(reference.value()), factory);
      }
    }
    return this;
  }

  /**
   * Registers a concrete class for serialization.
   *
   * @param type The serializable class.
   * @param id The serialization ID.
   * @return The serializer registry.
   * @throws RegistrationException If the given {@code type} is already registered or if no default
   *         serializer could be found for the given type.
   */
  public synchronized SerializerRegistry registerConcrete(Class<?> type, int id) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    return registerConcrete(type, id, findTypeSerializerFactory(type));
  }

  public SerializerRegistry registerConcrete(Class<?> concreteType, Class<? extends TypeSerializer> serializer) {
    return registerConcrete(concreteType, calculateTypeId(concreteType), new DefaultTypeSerializerFactory(serializer));
  }

  public SerializerRegistry registerConcrete(Class<?> concreteType, TypeSerializerFactory factory) {
    return registerConcrete(concreteType, calculateTypeId(concreteType), factory);
  }

  public SerializerRegistry registerConcrete(Class<?> concreteType, int typeId, Class<? extends TypeSerializer> serializer) {
    return registerConcrete(concreteType, typeId, new DefaultTypeSerializerFactory(serializer));
  }

  public synchronized SerializerRegistry registerConcrete(Class<?> concreteType, int typeId, TypeSerializerFactory factory) {
    if (concreteType == null)
      throw new NullPointerException("type cannot be null");

    // If the type ID has already been registered, throw an exception.
    if (types.containsKey(typeId) && types.get(typeId) != concreteType) {
      throw new RegistrationException("serializable type ID already registered: " + typeId);
    }

    // If the type has already been registered, throw an exception if the IDs don't match.
    if (ids.containsKey(concreteType)) {
      if (ids.get(concreteType) != typeId) {
        throw new RegistrationException("type registered with a different ID: " + concreteType);
      }
      return this;
    }

    factories.put(concreteType, factory);
    types.put(typeId, concreteType);
    ids.put(concreteType, typeId);

    return this;
  }

  /**
   * Registers the given class as an abstract type or serializer.
   *
   * @param type
   * @return
   */
  public SerializerRegistry registerAbstract(Class<?> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    if (isTypeSerializerClass(type)) {
      return registerAbstractSerializer(type);
    } else {
      return registerAbstract(type, calculateTypeId(type), findTypeSerializerFactory(type));
    }
  }

  /**
   * Registers an abstract serializer class.
   */
  private SerializerRegistry registerAbstractSerializer(Class<?> type) {
    validateAbstractSerializer(type);
    SerializerInfo info = findSerializerInfo(type);
    TypeSerializerFactory factory = findTypeSerializerFactory(type, info);
    for (SerializableTypeReference reference : info.value()) {
      if (reference.id() != 0) {
        registerAbstract(reference.value(), reference.id(), factory);
      } else {
        registerAbstract(reference.value(), calculateTypeId(reference.value()), factory);
      }
    }
    return this;
  }

  public SerializerRegistry registerAbstract(Class<?> abstractType, int typeId) {
    return registerAbstract(abstractType, typeId, findTypeSerializerFactory(abstractType));
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

  public SerializerRegistry registerDefault(Class<?> type) {
    if (type == null)
      throw new NullPointerException("type cannot be null");
    if (isTypeSerializerClass(type)) {
      return registerDefaultSerializer(type);
    } else {
      return registerDefault(type, findTypeSerializerFactory(type));
    }
  }

  /**
   * Registers a default serializer class.
   */
  private SerializerRegistry registerDefaultSerializer(Class<?> type) {
    validateDefaultSerializer(type);
    SerializerInfo info = findSerializerInfo(type);
    TypeSerializerFactory factory = findTypeSerializerFactory(type, info);
    for (SerializableTypeReference reference : info.value()) {
      registerDefault(reference.value(), factory);
    }
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

  /**
   * Returns a boolean indicating whether the given class is a type serializer class.
   */
  private boolean isTypeSerializerClass(Class<?> type) {
    return TypeSerializer.class.isAssignableFrom(type);
  }

  /**
   * Returns a type serializer factory for the given type serializer class.
   */
  @SuppressWarnings("unchecked")
  private TypeSerializerFactory findTypeSerializerFactory(Class<?> type, SerializerInfo info) {
    if (info != null) {
      if (info.factory() != DefaultTypeSerializerFactory.class) {
        try {
          return info.factory().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new CatalystException(e);
        }
      } else {
        return new DefaultTypeSerializerFactory((Class<? extends TypeSerializer>) type);
      }
    }
    return new DefaultTypeSerializerFactory((Class<? extends TypeSerializer>) type);
  }

  /**
   * Returns serializer info for the given type serializer class.
   */
  private SerializerInfo findSerializerInfo(Class<?> type) {
    return findAnnotation(type, SerializerInfo.class);
  }

  /**
   * Finds serializable type info for the given serializable type.
   */
  private SerializableTypeInfo findSerializableTypeInfo(Class<?> type) {
    return findAnnotation(type, SerializableTypeInfo.class);
  }

  /**
   * Finds the given annotation on the given type.
   */
  @SuppressWarnings("unchecked")
  private <T> T findAnnotation(Class<?> type, Class<? extends Annotation> annotation) {
    while (type != Object.class) {
      Annotation info = type.getAnnotation(annotation);
      if (info != null) {
        return (T) info;
      }
      for (Class<?> iface : type.getInterfaces()) {
        info = iface.getAnnotation(annotation);
        if (info != null) {
          return (T) info;
        }
      }
      type = type.getSuperclass();
    }
    return null;
  }

  /**
   * Finds a type serializer factory for the given serializable type.
   */
  private TypeSerializerFactory findTypeSerializerFactory(Class<?> type) {
    SerializableTypeInfo typeInfo = findSerializableTypeInfo(type);
    if (typeInfo == null || typeInfo.serializer() == SerializableTypeInfo.None.class) {
      Class<?> baseType = findBaseType(type, defaultFactories);
      if (baseType == null) {
        throw new RegistrationException("no default serializer found for type: " + type);
      }
      return defaultFactories.get(baseType);
    }

    Class<? extends TypeSerializer<?>> serializer = typeInfo.serializer();
    SerializerInfo serializerInfo = findSerializerInfo(serializer);
    if (serializerInfo == null) {
      return new DefaultTypeSerializerFactory(serializer);
    }
    return findTypeSerializerFactory(serializer, serializerInfo);
  }

  /**
   * Validates that a serializer class is a valid concrete serializer class.
   */
  private void validateConcreteSerializer(Class<?> serializer) {
    if (DefaultSerializer.class.isAssignableFrom(serializer) && !ConcreteSerializer.class.isAssignableFrom(serializer)) {
      throw new RegistrationException("cannot register default serializer as concrete");
    } else if (AbstractSerializer.class.isAssignableFrom(serializer) && !ConcreteSerializer.class.isAssignableFrom(serializer)) {
      throw new RegistrationException("cannot register abstract serializer as concrete");
    }

    SerializerInfo info = findSerializerInfo(serializer);
    if (info != null && info.scope() != TypeSerializer.Scope.NONE && info.scope() != TypeSerializer.Scope.CONCRETE) {
      throw new RegistrationException("cannot register " + info.scope() + " serializer as concrete");
    }
  }

  /**
   * Validates that a serializer class is a valid abstract serializer class.
   */
  private void validateAbstractSerializer(Class<?> serializer) {
    if (DefaultSerializer.class.isAssignableFrom(serializer) && !AbstractSerializer.class.isAssignableFrom(serializer)) {
      throw new RegistrationException("cannot register default serializer as abstract");
    } else if (ConcreteSerializer.class.isAssignableFrom(serializer)) {
      throw new RegistrationException("cannot register concrete serializer as abstract");
    }

    SerializerInfo info = findSerializerInfo(serializer);
    if (info != null && info.scope() != TypeSerializer.Scope.NONE && info.scope() != TypeSerializer.Scope.ABSTRACT) {
      throw new RegistrationException("cannot register " + info.scope() + " serializer as abstract");
    }
  }

  /**
   * Validates that a serializer class is a valid default serializer class.
   */
  private void validateDefaultSerializer(Class<?> serializer) {
    if (AbstractSerializer.class.isAssignableFrom(serializer)) {
      throw new RegistrationException("cannot register abstract serializer as default");
    } else if (ConcreteSerializer.class.isAssignableFrom(serializer)) {
      throw new RegistrationException("cannot register concrete serializer as default");
    }

    SerializerInfo info = findSerializerInfo(serializer);
    if (info != null && info.scope() != TypeSerializer.Scope.NONE && info.scope() != TypeSerializer.Scope.DEFAULT) {
      throw new RegistrationException("cannot register " + info.scope() + " serializer as default");
    }
  }

}
