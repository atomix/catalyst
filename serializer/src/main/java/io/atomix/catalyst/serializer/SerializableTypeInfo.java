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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Serializable type info.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SerializableTypeInfo {

  /**
   * The serializable type ID.
   */
  int id() default 0;

  /**
   * The serializer with which to serialize this type.
   */
  Class<? extends TypeSerializer<?>> serializer() default None.class;

  /**
   * Special {@link TypeSerializer} indicating no serializer for the type.
   */
  final class None implements TypeSerializer<Object> {
    @Override
    public void write(Object object, BufferOutput buffer, Serializer serializer) {
    }
    @Override
    public Object read(Class type, BufferInput buffer, Serializer serializer) {
      return null;
    }
  }

}
