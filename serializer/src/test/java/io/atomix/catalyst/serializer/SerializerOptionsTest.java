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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.PooledHeapAllocator;
import io.atomix.catalyst.serializer.collection.ArrayListSerializer;
import io.atomix.catalyst.serializer.util.BufferSerializer;
import io.atomix.catalyst.serializer.util.CatalystSerializableSerializer;
import io.atomix.catalyst.util.PropertiesReader;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.Assert.*;

/**
 * Serializer options test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class SerializerOptionsTest {

  /**
   * Tests serializer properties.
   */
  public void testProperties() throws Throwable {
    SerializerOptions options = new SerializerOptions(PropertiesReader.loadFromClasspath("serializer-test.properties").properties());
    assertTrue(options.whitelist());
    assertTrue(options.allocator() instanceof PooledHeapAllocator);
    assertEquals(options.types().get(1), ArrayList.class);
    assertEquals(options.serializers().get(1), ArrayListSerializer.class);
    assertEquals(options.types().get(2), Buffer.class);
    assertEquals(options.abstractSerializers().get(2), BufferSerializer.class);
    assertEquals(options.types().get(3), CatalystSerializable.class);
    assertEquals(options.defaultSerializers().get(3), CatalystSerializableSerializer.class);
  }

}
