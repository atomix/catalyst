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

import io.atomix.catalyst.serializer.SerializerRegistry;

import java.util.ServiceLoader;

/**
 * Serializable type resolver that loads child resolvers via {@link java.util.ServiceLoader}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ServiceLoaderTypeResolverResolver implements SerializableTypeResolver {
  private final ClassLoader classLoader;

  public ServiceLoaderTypeResolverResolver() {
    this(null);
  }

  public ServiceLoaderTypeResolverResolver(ClassLoader classLoader) {
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
    for (SerializableTypeResolver resolver : ServiceLoader.load(SerializableTypeResolver.class, classLoader)) {
      resolver.resolve(registry);
    }
  }

}
