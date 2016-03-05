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
 * limitations under the License
 */
package io.atomix.catalyst.transport;

import io.atomix.catalyst.transport.NettyOptions;
import io.atomix.catalyst.util.PropertiesReader;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;

/**
 * Netty properties test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class NettyOptionsTest {

  /**
   * Tests Netty property default values.
   */
  public void testPropertyDefaults() {
    NettyOptions options = new NettyOptions(new Properties());
    assertEquals(options.threads(), Runtime.getRuntime().availableProcessors());
    assertEquals(options.connectTimeout(), 5000);
    assertEquals(options.sendBufferSize(), -1);
    assertEquals(options.receiveBufferSize(), -1);
    assertEquals(options.reuseAddress(), true);
    assertEquals(options.tcpKeepAlive(), true);
    assertEquals(options.tcpNoDelay(), false);
    assertEquals(options.acceptBacklog(), 1024);
  }

  /**
   * Tests configuring Netty properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.put(NettyOptions.THREADS, "1");
    properties.put(NettyOptions.CONNECT_TIMEOUT, "1000");
    properties.put(NettyOptions.SEND_BUFFER_SIZE, "1024");
    properties.put(NettyOptions.RECEIVE_BUFFER_SIZE, "1024");
    properties.put(NettyOptions.REUSE_ADDRESS, "false");
    properties.put(NettyOptions.TCP_KEEP_ALIVE, "false");
    properties.put(NettyOptions.TCP_NO_DELAY, "true");
    properties.put(NettyOptions.ACCEPT_BACKLOG, "1234");

    NettyOptions options = new NettyOptions(properties);
    assertEquals(options.threads(), 1);
    assertEquals(options.connectTimeout(), 1000);
    assertEquals(options.sendBufferSize(), 1024);
    assertEquals(options.receiveBufferSize(), 1024);
    assertEquals(options.reuseAddress(), false);
    assertEquals(options.tcpKeepAlive(), false);
    assertEquals(options.tcpNoDelay(), true);
    assertEquals(options.acceptBacklog(), 1234);
  }

  /**
   * Tests configuring Netty properties from a properties file.
   */
  public void testPropertiesFile() {
    NettyOptions options = new NettyOptions(PropertiesReader.loadFromClasspath("netty-test.properties").properties());
    assertEquals(options.threads(), 1);
    assertEquals(options.connectTimeout(), 1000);
    assertEquals(options.sendBufferSize(), 1024);
    assertEquals(options.receiveBufferSize(), 1024);
    assertEquals(options.reuseAddress(), false);
    assertEquals(options.tcpKeepAlive(), false);
    assertEquals(options.tcpNoDelay(), true);
    assertEquals(options.acceptBacklog(), 1234);
  }

}
