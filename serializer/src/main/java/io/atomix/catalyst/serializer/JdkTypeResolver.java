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
 * limitations under the License.
 */
package io.atomix.catalyst.serializer;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.serializer.collection.ArrayListSerializer;
import io.atomix.catalyst.serializer.collection.HashMapSerializer;
import io.atomix.catalyst.serializer.collection.HashSetSerializer;
import io.atomix.catalyst.serializer.collection.MapEntrySerializer;
import io.atomix.catalyst.serializer.util.*;

import java.io.Externalizable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * JDK utilities type resolver.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JdkTypeResolver implements SerializableTypeResolver {
  @SuppressWarnings("unchecked")
  private static final Map<Class<?>, Class<? extends TypeSerializer<?>>> SERIALIZERS = new LinkedHashMap() {{
    put(BigInteger.class, BigIntegerSerializer.class);
    put(BigDecimal.class, BigDecimalSerializer.class);
    put(Date.class, DateSerializer.class);
    put(HashMap.class, HashMapSerializer.class);
    put(HashSet.class, HashSetSerializer.class);
    put(ArrayList.class, ArrayListSerializer.class);
  }};

  @SuppressWarnings("unchecked")
  private static final Map<Class<?>, Class<? extends TypeSerializer<?>>> ABSTRACT_SERIALIZERS = new LinkedHashMap() {{
    put(Buffer.class, BufferSerializer.class);
    put(Calendar.class, CalendarSerializer.class);
    put(TimeZone.class, TimeZoneSerializer.class);
    put(Map.Entry.class, MapEntrySerializer.class);
    put(Properties.class, PropertiesSerializer.class);
  }};

  @SuppressWarnings("unchecked")
  private static final Map<Class<?>, Class<? extends TypeSerializer<?>>> DEFAULT_SERIALIZERS = new LinkedHashMap() {{
    put(Serializable.class, JavaSerializableSerializer.class);
    put(Externalizable.class, ExternalizableSerializer.class);
    put(CatalystSerializable.class, CatalystSerializableSerializer.class);
  }};

  @Override
  public void resolve(SerializerRegistry registry) {
    int i = 176;
    for (Map.Entry<Class<?>, Class<? extends TypeSerializer<?>>> entry : SERIALIZERS.entrySet()) {
      registry.register(entry.getKey(), i++, entry.getValue());
    }

    i = 190;
    for (Map.Entry<Class<?>, Class<? extends TypeSerializer<?>>> entry : ABSTRACT_SERIALIZERS.entrySet()) {
      registry.registerAbstract(entry.getKey(), i++, entry.getValue());
    }

    for (Map.Entry<Class<?>, Class<? extends TypeSerializer<?>>> entry : DEFAULT_SERIALIZERS.entrySet()) {
      registry.registerDefault(entry.getKey(), entry.getValue());
    }
  }

}
