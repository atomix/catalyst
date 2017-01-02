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
package io.atomix.catalyst.serializer.kryo;

import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.kryo.GenericKryoSerializer;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Kryo serializer test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class GenericKryoSerializerTest {

  /**
   * Tests the Kryo serializer.
   */
  public void testSerializer() {
    Serializer serializer = new Serializer();
    GenericKryoSerializer kryoSerializer = new GenericKryoSerializer();
    Foo foo = new Foo(1234);
    HeapBuffer buffer = HeapBuffer.allocate();
    kryoSerializer.write(foo, buffer, serializer);
    Foo result = (Foo) kryoSerializer.read(Foo.class, buffer.flip(), serializer);
    assertEquals(result.bar, 1234);
  }

  /**
   * Tests that the Kryo serializer is used when registered with the Serializer.
   */
  public void testRegisteredSerializer() {
    Serializer serializer = new Serializer();
    serializer.register(Foo.class, 1, t -> new GenericKryoSerializer());
    HeapBuffer buffer = HeapBuffer.allocate();
    Foo foo = new Foo(1234);
    serializer.writeObject(foo, buffer);
    buffer.flip();
    Foo result = serializer.readObject(buffer);
    assertEquals(result.bar, 1234);
  }

  public static class Foo {
    private long bar;
    private Foo() {
    }
    private Foo(long bar) {
      this.bar = bar;
    }
  }

}
