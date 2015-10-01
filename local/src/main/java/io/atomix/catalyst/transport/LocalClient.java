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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalClient implements Client {
  private final UUID id = UUID.randomUUID();
  private final LocalServerRegistry registry;
  private final ThreadContext context;
  private final Set<LocalConnection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * @throws NullPointerException if any argument is null
   */
  public LocalClient(LocalServerRegistry registry, Serializer serializer) {
    Assert.notNull(registry, "registry");
    Assert.notNull(serializer, "serializer");
    this.registry = registry;
    this.context = new SingleThreadContext("local-client-" + id.toString(), serializer.clone());
  }

  /**
   * Returns the current execution context.
   */
  private ThreadContext getContext() {
    return ThreadContext.currentContextOrThrow();
  }

  @Override
  public CompletableFuture<Connection> connect(Address address) {
    Assert.notNull(address, "address");
    ThreadContext context = getContext();
    LocalServer server = registry.get(address);
    if (server == null) {
      return Futures.exceptionalFutureAsync(new TransportException("failed to connect"), context.executor());
    }

    LocalConnection connection = new LocalConnection(this.context, connections);
    connections.add(connection);
    return server.connect(connection).thenApplyAsync(v -> connection, context.executor());
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();

    ThreadContext context = getContext();
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    int i = 0;
    for (LocalConnection connection : connections) {
      futures[i++] = connection.close();
    }
    CompletableFuture.allOf(futures).thenRunAsync(() -> future.complete(null), context.executor());
    return future;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof LocalClient && ((LocalClient) object).id.equals(id);
  }

}
