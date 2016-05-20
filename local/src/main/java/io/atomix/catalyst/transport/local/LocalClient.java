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
package io.atomix.catalyst.transport.local;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.ComposableFuture;
import io.atomix.catalyst.concurrent.Futures;
import io.atomix.catalyst.concurrent.ThreadContext;

import java.net.ConnectException;
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
  private final Set<LocalConnection> connections = Collections.newSetFromMap(new ConcurrentHashMap<>());

  /**
   * @throws NullPointerException if any argument is null
   */
  public LocalClient(LocalServerRegistry registry) {
    this.registry = Assert.notNull(registry, "registry");
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
      return Futures.exceptionalFutureAsync(new ConnectException("failed to connect"), context.executor());
    }

    LocalConnection connection = new LocalConnection(context, connections);
    connections.add(connection);

    CompletableFuture<Connection> future = new CompletableFuture<>();
    server.connect(connection).whenCompleteAsync((result, error) -> {
      if (error == null) {
        future.complete(connection);
      } else {
        future.completeExceptionally(error);
      }
    }, context.executor());
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    ComposableFuture<Void> future = new ComposableFuture<>();

    ThreadContext context = getContext();
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    int i = 0;
    for (LocalConnection connection : connections) {
      futures[i++] = connection.close();
    }

    CompletableFuture.allOf(futures).whenCompleteAsync(future, context.executor());
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
