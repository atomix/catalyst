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
import io.atomix.catalyst.util.concurrent.CatalystThreadFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ThreadFactory;

/**
 * Netty protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTransport implements Transport {
  private final EventLoopGroup eventLoopGroup;

  public NettyTransport() {
    this(Runtime.getRuntime().availableProcessors());
  }

  /**
   * @throws IllegalArgumentException if {@code threads} is not positive
   */
  public NettyTransport(int threads) {
    Assert.arg(threads > 0, "threads must be positive");

    ThreadFactory threadFactory = new CatalystThreadFactory("catalyst-event-loop-%d");
    if (Epoll.isAvailable()) {
      eventLoopGroup = new EpollEventLoopGroup(threads, threadFactory);
    } else {
      eventLoopGroup = new NioEventLoopGroup(threads, threadFactory);
    }
  }

  @Override
  public Client client() {
    return new NettyClient(eventLoopGroup);
  }

  @Override
  public Server server() {
    return new NettyServer(eventLoopGroup);
  }

  @Override
  public void close() {
    try {
      eventLoopGroup.shutdownGracefully().sync();
    } catch (InterruptedException e) {
    }
  }

}
