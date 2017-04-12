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

import io.atomix.catalyst.buffer.UnsafeHeapBuffer;
import io.atomix.catalyst.serializer.Serializer;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Jackson serializer test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class GenericJacksonSerializerTest {

  /**
   * Tests the Kryo serializer.
   */
  public void testSerializer() {
    Serializer serializer = new Serializer();
    GenericJacksonSerializer jacksonSerializer = new GenericJacksonSerializer();
    Foo foo = new Foo(1234);
    UnsafeHeapBuffer buffer = UnsafeHeapBuffer.allocate();
    jacksonSerializer.write(foo, buffer, serializer);
    Foo result = (Foo) jacksonSerializer.read(Foo.class, buffer.flip(), serializer);
    assertEquals(result.bar, 1234);
  }

  public static class Foo {
    private long bar;
    private Foo() {
    }
    private Foo(long bar) {
      this.bar = bar;
    }

    public long getBar() {
      return bar;
    }

    public void setBar(long bar) {
      this.bar = bar;
    }
  }

}
