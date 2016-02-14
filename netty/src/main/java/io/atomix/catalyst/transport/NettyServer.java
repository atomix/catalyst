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

import io.atomix.catalyst.transport.NettyTls;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.handler.ssl.SslHandler;
import javax.net.ssl.SSLEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Netty server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyServer implements Server {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);
  private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
  private static final ChannelHandler FIELD_PREPENDER = new LengthFieldPrepender(2);

  private final NettyTransport transport;
  private final Map<Channel, NettyConnection> connections = new ConcurrentHashMap<>();
  private ServerHandler handler;
  private ChannelGroup channelGroup;
  private NettyTls nettytls;
  private volatile boolean listening;
  private CompletableFuture<Void> listenFuture;

  public NettyServer(NettyTransport transport) {
    this.transport = Assert.notNull(transport, "transport");
  }

  @Override
  public CompletableFuture<Void> listen(Address address, Consumer<Connection> listener) {
    Assert.notNull(address, "address");
    Assert.notNull(listener, "listener");
    if (listening)
      return CompletableFuture.completedFuture(null);

    ThreadContext context = ThreadContext.currentContextOrThrow();
    synchronized (this) {
      if (listenFuture == null) {
        listenFuture = new CompletableFuture<>();
        listen(address, listener, context);
      }
    }
    return listenFuture;
  }

  /**
   * Starts listening for the given member.
   */
  private void listen(Address address, Consumer<Connection> listener, ThreadContext context) {
    channelGroup = new DefaultChannelGroup("catalyst-acceptor-channels", GlobalEventExecutor.INSTANCE);

    handler = new ServerHandler(connections, listener, context);

    final ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(transport.eventLoopGroup())
      .channel(NioServerSocketChannel.class)
      .handler(new LoggingHandler(LogLevel.DEBUG))
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (transport.properties().sslEnabled() == true) {
            pipeline.addFirst(new SslHandler(new NettyTls(transport.properties()).InitSSLEngine(false)));
          }
          pipeline.addLast(FIELD_PREPENDER);
          pipeline.addLast(new LengthFieldBasedFrameDecoder(1024 * 64, 0, 2, 0, 2));
          pipeline.addLast(handler);
        }
      })
      .option(ChannelOption.SO_BACKLOG, transport.properties().acceptBacklog())
      .option(ChannelOption.TCP_NODELAY, transport.properties().tcpNoDelay())
      .option(ChannelOption.SO_REUSEADDR, transport.properties().reuseAddress())
      .childOption(ChannelOption.ALLOCATOR, ALLOCATOR)
      .childOption(ChannelOption.SO_KEEPALIVE, transport.properties().tcpKeepAlive());

    if (transport.properties().sendBufferSize() != -1) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, transport.properties().sendBufferSize());
    }
    if (transport.properties().receiveBufferSize() != -1) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, transport.properties().receiveBufferSize());
    }

    LOGGER.info("Binding to {}", address);

    ChannelFuture bindFuture = bootstrap.bind(address.socketAddress());
    bindFuture.addListener((ChannelFutureListener) channelFuture -> {
      if (channelFuture.isSuccess()) {
        listening = true;
        context.executor().execute(() -> {
          LOGGER.info("Listening at {}", bindFuture.channel().localAddress());
          listenFuture.complete(null);
        });
      } else {
        context.execute(() -> listenFuture.completeExceptionally(channelFuture.cause()));
      }
    });
    channelGroup.add(bindFuture.channel());
  }

  @Override
  public CompletableFuture<Void> close() {
    int i = 0;
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    for (Connection connection : connections.values()) {
      futures[i++] = connection.close();
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    CompletableFuture.allOf(futures).whenComplete((result, error) -> {
      channelGroup.close().addListener(channelFuture -> {
        future.complete(null);
      });
    });
    return future;
  }

  /**
   * Server handler.
   */
  @ChannelHandler.Sharable
  private static class ServerHandler extends NettyHandler {
    private ServerHandler(Map<Channel, NettyConnection> connections, Consumer<Connection> listener, ThreadContext context) {
      super(connections, listener, context);
    }
  }

}
