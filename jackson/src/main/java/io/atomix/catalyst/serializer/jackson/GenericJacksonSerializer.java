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
package io.atomix.catalyst.serializer.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferInputStream;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.buffer.BufferOutputStream;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;

import java.io.IOException;

/**
 * Jackson serializer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GenericJacksonSerializer implements TypeSerializer {
  private final ObjectMapper mapper;

  public GenericJacksonSerializer() {
    mapper = new ObjectMapper();
    mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
  }

  @Override
  public void write(Object object, BufferOutput buffer, Serializer serializer) {
    try {
      mapper.writeValue(new BufferOutputStream(buffer), object);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public Object read(Class type, BufferInput buffer, Serializer serializer) {
    try {
      return mapper.readValue(new BufferInputStream(buffer), type);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
  }

}
