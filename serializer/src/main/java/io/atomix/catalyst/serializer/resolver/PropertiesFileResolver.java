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
package io.atomix.catalyst.serializer.resolver;

import io.atomix.catalyst.CatalystException;
import io.atomix.catalyst.serializer.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Serializable type resolver that resolves serializable types using properties files.
 * <p>
 * This type resolver resolves serializable types and serializers via {@link java.util.ServiceLoader}. It searches the
 * classpath for serializable types and serializers registered as implementations of {@link CatalystSerializable},
 * {@link TypeSerializer}, or {@link TypeSerializerFactory}. Serializables can be annotated
 * with the {@link SerializableTypeInfo} annotation to identify serializable type IDs and serializers, and
 * serializers and factories can be annotated with the {@link SerializerInfo} annotation to identify
 * serializable types and IDs.
 * <p>
 * All classes that are loadable as services must provide a default no-argument constructor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class PropertiesFileResolver implements SerializableTypeResolver {
  private final ClassLoader classLoader;

  public PropertiesFileResolver() {
    this(null);
  }

  public PropertiesFileResolver(ClassLoader classLoader) {
    if (classLoader == null) {
      classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
    }
    this.classLoader = classLoader;
  }

  @Override
  public void resolve(SerializerRegistry registry) {
    resolveSerializers(registry);
    resolveSerializables(registry);
  }

  /**
   * Loads a list of classes from the classpath.
   */
  @SuppressWarnings("unchecked")
  private <T> List<Class<? extends T>> load(Class<T> clazz) {
    Enumeration<URL> urls;
    try {
      urls = classLoader.getResources(String.format("META-INF/services/%s", clazz.getName()));
    } catch (IOException e) {
      throw new CatalystException(e);
    }

    List<Class<? extends T>> classes = new ArrayList<>();
    while (urls.hasMoreElements()) {
      URL url = urls.nextElement();
      try {
        InputStream is = url.openStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
          int comment = line.lastIndexOf('#');
          if (comment >= 0) line = line.substring(0, line.lastIndexOf('#'));
          line = line.trim();
          if (line.length() > 0) {
            try {
              classes.add((Class<? extends T>) classLoader.loadClass(line));
            } catch (ClassNotFoundException e) {
              throw new CatalystException("failed to load class: " + line, e);
            }
          }
        }
        is.close();
      } catch (IOException | IllegalArgumentException | SecurityException e) {
        throw new CatalystException("failed to read services", e);
      }
    }
    return classes;
  }

  /**
   * Resolves serializers.
   */
  @SuppressWarnings("rawtypes")
  private void resolveSerializers(SerializerRegistry registry) {
    for (Class<? extends TypeSerializer> serializer : load(TypeSerializer.class)) {
      TypeSerializer.Scope scope = resolveScope(serializer);
      SerializerInfo serializerInfo = serializer.getAnnotation(SerializerInfo.class);
      if (serializerInfo != null) {
        for (SerializableTypeReference typeReference : serializerInfo.value()) {
          if (typeReference.id() != 0) {
            registry.register(typeReference.value(), typeReference.id(), serializer, scope);
          } else {
            registry.register(typeReference.value(), serializer, scope);
          }
        }
      }
    }
  }

  /**
   * Resolves serializables.
   */
  private void resolveSerializables(SerializerRegistry registry) {
    for (Class<? extends CatalystSerializable> serializable : load(CatalystSerializable.class)) {
      SerializableTypeInfo typeInfo = serializable.getAnnotation(SerializableTypeInfo.class);
      if (typeInfo != null) {
        if (typeInfo.serializer() != null && typeInfo.id() != 0) {
          registry.registerConcrete(serializable, typeInfo.id(), typeInfo.serializer());
        } else if (typeInfo.serializer() != null) {
          registry.registerConcrete(serializable, typeInfo.serializer());
        } else if (typeInfo.id() != 0) {
          registry.registerConcrete(serializable, typeInfo.id());
        } else {
          registry.registerConcrete(serializable);
        }
      } else {
        registry.registerConcrete(serializable);
      }
    }
  }

  /**
   * Resolves the scope for the given type.
   */
  private TypeSerializer.Scope resolveScope(Class<? extends TypeSerializer> type) {
    if (DefaultSerializer.class.isAssignableFrom(type)) {
      return TypeSerializer.Scope.DEFAULT;
    } else if (AbstractSerializer.class.isAssignableFrom(type)) {
      return TypeSerializer.Scope.ABSTRACT;
    } else {
      return TypeSerializer.Scope.CONCRETE;
    }
  }

}
