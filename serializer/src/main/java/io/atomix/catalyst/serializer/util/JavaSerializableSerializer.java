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
package io.atomix.catalyst.serializer.util;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;

import java.io.*;

/**
 * Java serializable serializer implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class JavaSerializableSerializer<T> implements TypeSerializer<T> {

  @Override
  public void write(T object, BufferOutput buffer, Serializer serializer) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream(); ObjectOutputStream out = new ObjectOutputStream(os)) {
      out.writeObject(object);
      out.flush();
      byte[] bytes = os.toByteArray();
      buffer.writeUnsignedShort(bytes.length).write(bytes);
    } catch (IOException e) {
      throw new SerializationException("failed to serialize Java object", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read(Class<T> type, BufferInput buffer, Serializer serializer) {
    byte[] bytes = new byte[buffer.readUnsignedShort()];
    buffer.read(bytes);
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      try {
        return (T) in.readObject();
      } catch (ClassNotFoundException e) {
        throw new SerializationException("failed to deserialize Java object", e);
      }
    } catch (IOException e) {
      throw new SerializationException("failed to deserialize Java object", e);
    }
  }

}
