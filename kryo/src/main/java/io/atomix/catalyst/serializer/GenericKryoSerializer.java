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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferInputStream;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.buffer.BufferOutputStream;

/**
 * Catalyst Kryo serializer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GenericKryoSerializer implements TypeSerializer {
  private final Kryo kryo = new Kryo();

  @Override
  public void write(Object object, BufferOutput buffer, Serializer serializer) {
    Output output = new Output(new BufferOutputStream(buffer));
    kryo.writeClassAndObject(output, object);
    output.flush();
  }

  @Override
  public Object read(Class type, BufferInput buffer, Serializer serializer) {
    Input input = new Input(new BufferInputStream(buffer));
    return kryo.readClassAndObject(input);
  }

}
