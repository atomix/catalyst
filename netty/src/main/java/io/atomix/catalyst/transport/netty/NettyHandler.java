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
package io.atomix.catalyst.transport.netty;

import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Connection;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Netty handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class NettyHandler extends ChannelInboundHandlerAdapter {
  private final Map<Channel, NettyConnection> connections;
  private final Consumer<Connection> listener;
  private final ThreadContext context;
  private final NettyOptions options;

  protected NettyHandler(Map<Channel, NettyConnection> connections, Consumer<Connection> listener, ThreadContext context, NettyOptions options) {
    this.connections = connections;
    this.listener = listener;
    this.context = context;
    this.options = options;
  }

  /**
   * Adds a connection for the given channel.
   *
   * @param channel The channel for which to add the connection.
   * @param connection The connection to add.
   */
  protected void setConnection(Channel channel, NettyConnection connection) {
    connections.put(channel, connection);
  }

  /**
   * Returns the connection for the given channel.
   *
   * @param channel The channel for which to return the connection.
   * @return The connection.
   */
  protected NettyConnection getConnection(Channel channel) {
    return connections.get(channel);
  }

  /**
   * Removes the connection for the given channel.
   *
   * @param channel The channel for which to remove the connection.
   * @return The connection.
   */
  protected NettyConnection removeConnection(Channel channel) {
    return connections.remove(channel);
  }

  /**
   * Returns the current execution context or creates one.
   */
  private ThreadContext getOrCreateContext(Channel channel) {
    ThreadContext context = ThreadContext.currentContext();
    if (context != null) {
      return context;
    }
    return new SingleThreadContext(channel.eventLoop(), this.context.serializer().clone());
  }

  @Override
  public void channelActive(ChannelHandlerContext context) throws Exception {
    Channel channel = context.channel();
    NettyConnection connection = new NettyConnection(channel, getOrCreateContext(channel), options);
    setConnection(channel, connection);
    // synchronously notify listeners in order to ensure message handlers
    // are registered before messages are handled.
    CompletableFuture.runAsync(() -> listener.accept(connection), this.context.executor()).join();
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, Object message) {
    ByteBuf buffer = (ByteBuf) message;
    int type = buffer.readByte();
    switch (type) {
      case NettyConnection.REQUEST:
        handleRequest(buffer, context);
        break;
      case NettyConnection.RESPONSE:
        handleResponse(buffer, context);
        break;
    }
  }

  /**
   * Handles a request.
   */
  private void handleRequest(ByteBuf request, ChannelHandlerContext context) {
    NettyConnection connection = getConnection(context.channel());
    if (connection != null) {
      connection.handleRequest(request);
    }
  }

  /**
   * Handles a response.
   */
  private void handleResponse(ByteBuf response, ChannelHandlerContext context) {
    NettyConnection connection = getConnection(context.channel());
    if (connection != null) {
      connection.handleResponse(response);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, final Throwable t) throws Exception {
    Channel channel = context.channel();
    NettyConnection connection = getConnection(channel);
    if (connection != null) {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (Throwable ignore) {
      }
      connection.handleException(t);
    } else {
      channel.close();
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    Channel channel = context.channel();
    NettyConnection connection = removeConnection(channel);
    if (connection != null) {
      connection.handleClosed();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext context, Object event) throws Exception {
    if (event instanceof IdleStateEvent && ((IdleStateEvent) event).state() == IdleState.ALL_IDLE) {
      context.close();
    }
  }

}
