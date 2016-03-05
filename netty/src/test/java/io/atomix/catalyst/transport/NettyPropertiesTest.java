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
public class NettyPropertiesTest {

  /**
   * Tests Netty property default values.
   */
  public void testPropertyDefaults() {
    NettyProperties properties = new NettyProperties(new Properties());
    assertEquals(properties.threads(), Runtime.getRuntime().availableProcessors());
    assertEquals(properties.connectTimeout(), 5000);
    assertEquals(properties.sendBufferSize(), -1);
    assertEquals(properties.receiveBufferSize(), -1);
    assertEquals(properties.reuseAddress(), true);
    assertEquals(properties.tcpKeepAlive(), true);
    assertEquals(properties.tcpNoDelay(), false);
    assertEquals(properties.acceptBacklog(), 1024);
  }

  /**
   * Tests configuring Netty properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.put(NettyProperties.THREADS, "1");
    properties.put(NettyProperties.CONNECT_TIMEOUT, "1000");
    properties.put(NettyProperties.SEND_BUFFER_SIZE, "1024");
    properties.put(NettyProperties.RECEIVE_BUFFER_SIZE, "1024");
    properties.put(NettyProperties.REUSE_ADDRESS, "false");
    properties.put(NettyProperties.TCP_KEEP_ALIVE, "false");
    properties.put(NettyProperties.TCP_NO_DELAY, "true");
    properties.put(NettyProperties.ACCEPT_BACKLOG, "1234");

    NettyProperties nettyProperties = new NettyProperties(properties);
    assertEquals(nettyProperties.threads(), 1);
    assertEquals(nettyProperties.connectTimeout(), 1000);
    assertEquals(nettyProperties.sendBufferSize(), 1024);
    assertEquals(nettyProperties.receiveBufferSize(), 1024);
    assertEquals(nettyProperties.reuseAddress(), false);
    assertEquals(nettyProperties.tcpKeepAlive(), false);
    assertEquals(nettyProperties.tcpNoDelay(), true);
    assertEquals(nettyProperties.acceptBacklog(), 1234);
  }

  /**
   * Tests configuring Netty properties from a properties file.
   */
  public void testPropertiesFile() {
    NettyProperties properties = new NettyProperties(PropertiesReader.loadFromClasspath("netty-test.properties").properties());
    assertEquals(properties.threads(), 1);
    assertEquals(properties.connectTimeout(), 1000);
    assertEquals(properties.sendBufferSize(), 1024);
    assertEquals(properties.receiveBufferSize(), 1024);
    assertEquals(properties.reuseAddress(), false);
    assertEquals(properties.tcpKeepAlive(), false);
    assertEquals(properties.tcpNoDelay(), true);
    assertEquals(properties.acceptBacklog(), 1234);
  }

}
