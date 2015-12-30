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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.ReferenceCounted;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;

import java.util.Collections;
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
  private final Map<Class<?>, HandlerHolder> handlers = new ConcurrentHashMap<>();
  private final Listeners<Throwable> exceptionListeners = new Listeners<>();
  private final Listeners<Connection> closeListeners = new Listeners<>();
  private final Set<CompletableFuture<?>> futures = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private volatile boolean open = true;

  public LocalConnection(ThreadContext context) {
    this(context, null);
  }

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
    CompletableFuture<U> future = new CompletableFuture<>();
    ThreadContext context = ThreadContext.currentContextOrThrow();
    this.context.execute(() -> {
      Buffer requestBuffer = this.context.serializer().writeObject(request);
      connection.<U>receive(requestBuffer.flip()).whenComplete((responseBuffer, error) -> {
        if (error == null) {
          int status = responseBuffer.readByte();
          if (status == RESPONSE_OK) {
            U response = this.context.serializer().readObject(responseBuffer);
            context.execute(() -> future.complete(response));
          } else if (status == RESPONSE_ERROR) {
            Throwable exception = this.context.serializer().readObject(responseBuffer);
            context.execute(() -> future.completeExceptionally(exception));
          } else {
            context.execute(() -> future.completeExceptionally(new TransportException("malformed response")));
          }
          responseBuffer.release();
        } else {
          context.execute(() -> future.completeExceptionally(error));
        }
      });

      if (request instanceof ReferenceCounted) {
        ((ReferenceCounted<?>) request).release();
      }
    });
    return future;
  }

  /**
   * Receives a message.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Buffer> receive(Buffer requestBuffer) {
    ThreadContext context = ThreadContext.currentContextOrThrow();

    Object request = context.serializer().readObject(requestBuffer);
    requestBuffer.release();

    HandlerHolder holder = handlers.get(request.getClass());
    if (holder != null) {
      MessageHandler<Object, Object> handler = (MessageHandler<Object, Object>) holder.handler;
      CompletableFuture<Buffer> future = new CompletableFuture<>();

      try {
        holder.context.executor().execute(() -> {
          handler.handle(request).whenComplete((response, error) -> {
            if (!open || !connection.open) {
              future.completeExceptionally(new IllegalStateException("connection closed"));
            } else {
              respond(response, error, future, context);
            }
          });
        });
      } catch (RejectedExecutionException e) {
        future.completeExceptionally(new IllegalStateException("connection closed"));
      }

      futures.add(future);
      return future.whenComplete((result, error) -> futures.remove(future));
    }
    return Futures.exceptionalFuture(new TransportException("no handler registered"));
  }

  /**
   * Responds to a request.
   */
  private void respond(Object response, Throwable error, CompletableFuture<Buffer> future, ThreadContext context) {
    try {
      context.executor().execute(() -> {
        Buffer responseBuffer = context.serializer().allocate();
        if (error == null) {
          responseBuffer.writeByte(RESPONSE_OK);
          context.serializer().writeObject(response, responseBuffer);
        } else {
          responseBuffer.writeByte(RESPONSE_ERROR);
          context.serializer().writeObject(error, responseBuffer);
        }

        future.complete(responseBuffer.flip());

        if (response instanceof ReferenceCounted) {
          ((ReferenceCounted<?>) response).release();
        }
      });
    } catch (RejectedExecutionException e) {
      // Do nothing if the sending context is closed.
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
    doClose();
    connection.doClose();
    futures.forEach(f -> {
      if (!f.isDone()) {
        f.completeExceptionally(new IllegalStateException("connection closed"));
      }
    });
    return ThreadContext.currentContextOrThrow().execute(() -> null);
  }

  /**
   * Closes the connection.
   */
  private void doClose() {
    open = false;
    if (connections != null)
      connections.remove(this);

    for (Consumer<Connection> closeListener : closeListeners) {
      context.executor().execute(() -> closeListener.accept(this));
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
