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
package io.atomix.catalyst.transport;

import io.atomix.catalyst.buffer.PooledDirectAllocator;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

/**
 * Local transport.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalTransport implements Transport {
  private final LocalServerRegistry registry;
  private final Serializer serializer;

  public LocalTransport(LocalServerRegistry registry) {
    this(registry, new Serializer(new PooledDirectAllocator()));
  }

  /**
   * @throws NullPointerException if any argument is null
   */
  public LocalTransport(LocalServerRegistry registry, Serializer serializer) {
    Assert.notNull(registry, "registry");
    Assert.notNull(serializer, "serializer");
    this.registry = registry;
    this.serializer = serializer;
  }

  @Override
  public Client client() {
    return new LocalClient(registry, serializer);
  }

  @Override
  public Server server() {
    return new LocalServer(registry, serializer);
  }

}
