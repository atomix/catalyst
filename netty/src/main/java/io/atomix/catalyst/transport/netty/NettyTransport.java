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

import io.atomix.catalyst.concurrent.CatalystThreadFactory;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.Properties;
import java.util.concurrent.ThreadFactory;

/**
 * Netty protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTransport implements Transport {

  /**
   * Returns a new Netty transport builder.
   *
   * @return A new Netty transport builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final NettyOptions properties;
  private final EventLoopGroup eventLoopGroup;

  public NettyTransport() {
    this(new NettyOptions(new Properties()));
  }

  public NettyTransport(Properties properties) {
    this(new NettyOptions(properties));
  }

  public NettyTransport(NettyOptions properties) {
    this.properties = Assert.notNull(properties, "properties");
    ThreadFactory threadFactory = new CatalystThreadFactory("catalyst-event-loop-%d");
    eventLoopGroup = new NioEventLoopGroup(properties.threads(), threadFactory);
  }

  /**
   * Returns the Netty transport properties.
   *
   * @return The Netty transport properties.
   */
  public NettyOptions properties() {
    return properties;
  }

  /**
   * Returns the Netty event loop group.
   *
   * @return The Netty event loop group.
   */
  public EventLoopGroup eventLoopGroup() {
    return eventLoopGroup;
  }

  @Override
  public Client client() {
    return new NettyClient(this);
  }

  @Override
  public Server server() {
    return new NettyServer(this);
  }

  @Override
  public void close() {
    try {
      eventLoopGroup.shutdownGracefully().sync();
    } catch (InterruptedException e) {
    }
  }

  /**
   * Netty transport builder.
   */
  public static class Builder implements Transport.Builder {
    private final Properties properties = new Properties();

    private Builder() {
    }

    /**
     * Sets the number of Netty event loop threads.
     *
     * @param threads The number of Netty event loop threads.
     * @return The Netty transport builder.
     */
    public Builder withThreads(int threads) {
      properties.setProperty(NettyOptions.THREADS, String.valueOf(Assert.argNot(threads, threads <= 0, "threads must be positive")));
      return this;
    }

    /**
     * Sets the Netty connect timeout.
     *
     * @param timeout The connect timeout.
     * @return The Netty transport builder.
     */
    public Builder withConnectTimeout(int timeout) {
      properties.setProperty(NettyOptions.CONNECT_TIMEOUT, String.valueOf(Assert.argNot(timeout, timeout <= 0, "timeout must be positive")));
      return this;
    }

    /**
     * Sets the send buffer size.
     *
     * @param sendBufferSize The send buffer size.
     * @return The Netty transport builder.
     */
    public Builder withSendBufferSize(int sendBufferSize) {
      properties.setProperty(NettyOptions.SEND_BUFFER_SIZE, String.valueOf(Assert.argNot(sendBufferSize, sendBufferSize <= 0, "buffer size must be positive")));
      return this;
    }

    /**
     * Sets the receive buffer size.
     *
     * @param receiveBufferSize The receive buffer size.
     * @return The Netty transport builder.
     */
    public Builder withReceiveBufferSize(int receiveBufferSize) {
      properties.setProperty(NettyOptions.RECEIVE_BUFFER_SIZE, String.valueOf(Assert.argNot(receiveBufferSize, receiveBufferSize <= 0, "buffer size must be positive")));
      return this;
    }

    /**
     * Sets the maximum frame size.
     *
     * @param maxFrameSize The maximum frame size.
     * @return The Netty transport builder.
     */
    public Builder withMaxFrameSize(int maxFrameSize) {
      properties.setProperty(NettyOptions.MAX_FRAME_SIZE, String.valueOf(Assert.argNot(maxFrameSize, maxFrameSize <= 0, "maximum frame size must be positive")));
      return this;
    }

    /**
     * Enables the SO_REUSEADDR option.
     *
     * @return The Netty transport builder.
     */
    public Builder withReuseAddress() {
      return withReuseAddress(true);
    }

    /**
     * Sets the SO_REUSEADDR option.
     *
     * @param reuseAddress Whether to enable SO_REUSEADDR.
     * @return The Netty transport builder.
     */
    public Builder withReuseAddress(boolean reuseAddress) {
      properties.setProperty(NettyOptions.REUSE_ADDRESS, String.valueOf(reuseAddress));
      return this;
    }

    /**
     * Enables the SO_KEEPALIVE option.
     *
     * @return The Netty transport builder.
     */
    public Builder withTcpKeepAlive() {
      return withTcpKeepAlive(true);
    }

    /**
     * Sets the SO_KEEPALIVE option.
     *
     * @param tcpKeepAlive Whether to enable SO_KEEPALIVE.
     * @return The Netty transport builder.
     */
    public Builder withTcpKeepAlive(boolean tcpKeepAlive) {
      properties.setProperty(NettyOptions.TCP_KEEP_ALIVE, String.valueOf(tcpKeepAlive));
      return this;
    }

    /**
     * Enables the TCP_NODELAY option.
     *
     * @return The Netty transport builder.
     */
    public Builder withTcpNoDelay() {
      return withTcpNoDelay(true);
    }

    /**
     * Sets the TCP_NODELAY option.
     *
     * @param tcpNoDelay Whether to enable TCP_NODELAY.
     * @return The Netty transport builder.
     */
    public Builder withTcpNoDelay(boolean tcpNoDelay) {
      properties.setProperty(NettyOptions.TCP_NO_DELAY, String.valueOf(tcpNoDelay));
      return this;
    }

    /**
     * Sets the TCP accept backlog.
     *
     * @param acceptBacklog The accept backlog.
     * @return The Netty transport builder.
     */
    public Builder withAcceptBacklog(int acceptBacklog) {
      properties.setProperty(NettyOptions.ACCEPT_BACKLOG, String.valueOf(Assert.argNot(acceptBacklog, acceptBacklog <= 0, "accept backlog must be positive")));
      return this;
    }

    /**
     * Enables SSL.
     *
     * @return The Netty transport builder.
     */
    public Builder withSsl() {
      return withSsl(true);
    }

    /**
     * Sets whether to enable SSL.
     *
     * @param sslEnabled Whether to enable SSL.
     * @return The Netty transport builder.
     */
    public Builder withSsl(boolean sslEnabled) {
      properties.setProperty(NettyOptions.SSL_ENABLED, String.valueOf(sslEnabled));
      return this;
    }

    /**
     * Sets the SSL protocol.
     *
     * @param sslProtocol The SSL protocol.
     * @return The Netty transport builder.
     */
    public Builder withSslProtocol(SslProtocol sslProtocol) {
      properties.setProperty(NettyOptions.SSL_PROTOCOL, Assert.notNull(sslProtocol, "sslProtocol").name().replace("_", "."));
      return this;
    }

    /**
     * Sets the SSL trust store path.
     *
     * @param trustStorePath The trust store path.
     * @return The Netty transport builder.
     */
    public Builder withTrustStorePath(String trustStorePath) {
      properties.setProperty(NettyOptions.SSL_TRUST_STORE_PATH, Assert.notNull(trustStorePath, "trustStorePath"));
      return this;
    }

    /**
     * Sets the SSL trust store password.
     *
     * @param trustStorePassword The trust store password.
     * @return The Netty transport builder.
     */
    public Builder withTrustStorePassword(String trustStorePassword) {
      properties.setProperty(NettyOptions.SSL_TRUST_STORE_PASSWORD, Assert.notNull(trustStorePassword, "trustStorePassword"));
      return this;
    }

    /**
     * Sets the SSL key store path.
     *
     * @param keyStorePath The key store path.
     * @return The Netty transport builder.
     */
    public Builder withKeyStorePath(String keyStorePath) {
      properties.setProperty(NettyOptions.SSL_KEY_STORE_PATH, Assert.notNull(keyStorePath, "keyStorePath"));
      return this;
    }

    /**
     * Sets the SSL key store password.
     *
     * @param trustStorePassword The key store password.
     * @return The Netty transport builder.
     */
    public Builder withKeyStorePassword(String trustStorePassword) {
      properties.setProperty(NettyOptions.SSL_KEY_STORE_PASSWORD, Assert.notNull(trustStorePassword, "trustStorePassword"));
      return this;
    }

    /**
     * Sets the SSL key store key password.
     *
     * @param keyStoreKeyPassword The key store key password.
     * @return The Netty transport builder.
     */
    public Builder withKeyStoreKeyPassword(String keyStoreKeyPassword) {
      properties.setProperty(NettyOptions.SSL_KEY_STORE_KEY_PASSWORD, Assert.notNull(keyStoreKeyPassword, "trustStorePassword"));
      return this;
    }

    @Override
    public Transport build() {
      return new NettyTransport(properties);
    }
  }

}
