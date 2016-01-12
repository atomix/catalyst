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

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.ReferenceCounted;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Local connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalConnection implements Connection {
  private static final int RESPONSE_ERROR = 0x00;
  private static final int RESPONSE_OK = 0x01;
  private final UUID id = UUID.randomUUID();
  private final ThreadContext context;
  private final Set<LocalConnection> connections;
  private LocalConnection connection;
  private long requestId;
  private final Map<Long, ContextualFuture> futures = new ConcurrentHashMap<>();
  private final Map<Class<?>, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Listeners<Throwable> exceptionListeners = new Listeners<>();
  private final Listeners<Connection> closeListeners = new Listeners<>();
  volatile boolean open = true;

  public LocalConnection(ThreadContext context, Set<LocalConnection> connections) {
    this.context = context;
    this.connections = connections;
  }

  /**
   * Connects the connection to another connection.
   */
  public LocalConnection connect(LocalConnection connection) {
    this.connection = connection;
    return this;
  }

  @Override
  public <T, U> CompletableFuture<U> send(T request) {
    if (!open || !connection.open)
      return Futures.exceptionalFuture(new IllegalStateException("connection closed"));

    Assert.notNull(request, "request");

    ContextualFuture<U> future = new ContextualFuture<>(ThreadContext.currentContextOrThrow());
    this.context.execute(() -> sendRequest(request, future));
    return future;
  }

  /**
   * Sends a request.
   */
  private void sendRequest(Object request, ContextualFuture future) {
    if (open && connection.open) {
      long requestId = ++this.requestId;
      futures.put(requestId, future);
      connection.handleRequest(requestId, request);
    } else {
      future.context.executor().execute(() -> future.completeExceptionally(new IllegalStateException("connection closed")));
    }

    if (request instanceof ReferenceCounted) {
      ((ReferenceCounted<?>) request).release();
    }
  }

  /**
   * Handles a request response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponseOk(long requestId, Object response) {
    ContextualFuture future = futures.remove(requestId);
    if (future != null) {
      future.context.executor().execute(() -> future.complete(response));
    }
  }

  /**
   * Handles a response error.
   */
  private void handleResponseError(long requestId, Throwable error) {
    ContextualFuture future = futures.remove(requestId);
    if (future != null) {
      future.context.execute(() -> future.completeExceptionally(error));
    }
  }

  /**
   * Receives a message.
   */
  @SuppressWarnings("unchecked")
  private void handleRequest(long requestId, Object request) {
    HandlerHolder holder = handlers.get(request.getClass());
    if (holder == null) {
      connection.handleResponseError(requestId, new TransportException("no handler registered"));
      return;
    }

    MessageHandler<Object, Object> handler = (MessageHandler<Object, Object>) holder.handler;

    try {
      holder.context.executor().execute(() -> {
        if (open && connection.open) {
          handler.handle(request).whenComplete((response, error) -> {
            if (!open || !connection.open) {
              connection.handleResponseError(requestId, new IllegalStateException("connection closed"));
            } else if (error == null) {
              connection.handleResponseOk(requestId, response);
            } else {
              connection.handleResponseError(requestId, error);
            }
          });
        } else {
          connection.handleResponseError(requestId, new IllegalStateException("connection closed"));
        }
      });
    } catch (RejectedExecutionException e) {
      connection.handleResponseError(requestId, new IllegalStateException("connection closed"));
    }
  }

  @Override
  public <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler) {
    Assert.notNull(type, "type");
    if (handler != null) {
      handlers.put(type, new HandlerHolder(handler, ThreadContext.currentContextOrThrow()));
    } else {
      handlers.remove(type);
    }
    return this;
  }

  @Override
  public Listener<Throwable> exceptionListener(Consumer<Throwable> listener) {
    return exceptionListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public Listener<Connection> closeListener(Consumer<Connection> listener) {
    return closeListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);
    doClose();
    connection.doClose();
    return ThreadContext.currentContextOrThrow().execute(() -> null);
  }

  /**
   * Closes the connection.
   */
  private void doClose() {
    open = false;
    connections.remove(this);

    for (Map.Entry<Long, ContextualFuture> entry : futures.entrySet()) {
      ContextualFuture future = entry.getValue();
      try {
        future.context.executor().execute(() -> future.completeExceptionally(new IllegalStateException("connection closed")));
      } catch (RejectedExecutionException e) {
      }
    }
    futures.clear();

    for (Consumer<Connection> closeListener : closeListeners) {
      try {
        context.executor().execute(() -> closeListener.accept(this));
      } catch (RejectedExecutionException e) {
      }
    }
  }

  /**
   * Contextual future.
   */
  private static class ContextualFuture<T> extends CompletableFuture<T> {
    private final ThreadContext context;

    private ContextualFuture(ThreadContext context) {
      this.context = context;
    }
  }

  /**
   * Holds message handler and thread context.
   */
  protected static class HandlerHolder {
    private final MessageHandler<?, ?> handler;
    private final ThreadContext context;

    private HandlerHolder(MessageHandler<?, ?> handler, ThreadContext context) {
      this.handler = handler;
      this.context = context;
    }
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof LocalConnection && ((LocalConnection) object).id.equals(id);
  }

}
