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
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Netty client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyClient implements Client {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
  private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
  private static final ChannelHandler FIELD_PREPENDER = new LengthFieldPrepender(2);

  private final NettyTransport transport;
  private final Map<Channel, NettyConnection> connections = new ConcurrentHashMap<>();

  /**
   * @throws NullPointerException if {@code id} or {@code eventLoopGroup} are null 
   */
  public NettyClient(NettyTransport transport) {
    this.transport = Assert.notNull(transport, "transport");
  }

  @Override
  public CompletableFuture<Connection> connect(Address address) {
    Assert.notNull(address, "address");
    ThreadContext context = ThreadContext.currentContextOrThrow();
    CompletableFuture<Connection> future = new ComposableFuture<>();

    LOGGER.info("Connecting to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(transport.eventLoopGroup())
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          pipeline.addLast(FIELD_PREPENDER);
          pipeline.addLast(new LengthFieldBasedFrameDecoder(1024 * 64, 0, 2, 0, 2));
          pipeline.addLast(new NettyHandler(connections, future::complete, context));
        }
      });

    bootstrap.option(ChannelOption.TCP_NODELAY, transport.properties().tcpNoDelay());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, transport.properties().tcpKeepAlive());
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, transport.properties().connectTimeout());
    bootstrap.option(ChannelOption.ALLOCATOR, ALLOCATOR);

    if (transport.properties().sendBufferSize() != -1) {
      bootstrap.option(ChannelOption.SO_SNDBUF, transport.properties().sendBufferSize());
    }
    if (transport.properties().receiveBufferSize() != -1) {
      bootstrap.option(ChannelOption.SO_RCVBUF, transport.properties().receiveBufferSize());
    }

    bootstrap.connect(address.socketAddress()).addListener(channelFuture -> {
      if (channelFuture.isSuccess()) {
        LOGGER.info("Connected to {}", address);
      } else {
        context.execute(() -> future.completeExceptionally(channelFuture.cause()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    ThreadContext.currentContextOrThrow();
    int i = 0;
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    for (Connection connection : connections.values()) {
      futures[i++] = connection.close();
    }
    return CompletableFuture.allOf(futures);
  }

}
