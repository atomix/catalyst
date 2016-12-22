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

import io.atomix.catalyst.CatalystException;
import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.StringHasher;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer registry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SerializerRegistry2 {
  private final Hasher hasher = new StringHasher();
  private final Map<Class<?>, TypeSerializerFactory> factories = new ConcurrentHashMap<>();
  private final Map<Class<?>, TypeSerializerFactory> abstractFactories = Collections.synchronizedMap(new LinkedHashMap<>(1024, 0.75f, true));
  private final Map<Class<?>, TypeSerializerFactory> defaultFactories = Collections.synchronizedMap(new LinkedHashMap<>(1024, 0.75f, true));
  private final Map<Class<?>, Integer> ids = new ConcurrentHashMap<>();
  private final Map<Integer, Class<?>> types = new ConcurrentHashMap<>();

  public SerializerRegistry2 register(Class<?> type) {
    return register(type, findTypeId(type), findTypeSerializer(type));
  }

  public SerializerRegistry2 register(Class<?> type, Class<? extends TypeSerializer> serializer) {

  }

  public SerializerRegistry2 register(Class<?> type, int typeId, Class<? extends TypeSerializer> serializer) {

  }

  public SerializerRegistry2 registerConcrete(Class<?> type, int typeId, Class<? extends TypeSerializer> serializer) {

  }

  public SerializerRegistry2 registerAbstract(Class<?> type, int typeId, Class<? extends TypeSerializer> serializer) {

  }

  public SerializerRegistry2 registerDefault(Class<?> baseType, Class<? extends TypeSerializer> serializer) {

  }

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

  private SerializerInfo findSerializerInfo(Class<?> type) {
    return findAnnotation(type, SerializerInfo.class);
  }

  private SerializableTypeInfo findSerializableTypeInfo(Class<?> type) {
    return findAnnotation(type, SerializableTypeInfo.class);
  }

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

}
