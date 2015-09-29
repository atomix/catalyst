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

/**
 * Transport provider.
 * <p>
 * This is a low level abstraction that serves to provide Catalyst clients and servers with {@link Client}
 * and {@link Server} instances. Throughout the lifetime of a {@code Transport}, Catalyst may
 * call on the transport to create multiple {@link Client} and {@link Server}
 * objects. Each {@code Client} and {@code Server} is identified by a {@link java.util.UUID} which is assumed to be unique
 * to that instance across the entire Catalyst cluster.
 * <p>
 * When the {@link Transport} is closed, it should close all {@link Client}
 * and {@link Server} instances created by it if they're not already closed.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Transport {

  /**
   * Creates a transport client.
   *
   * @return The transport client.
   * @throws NullPointerException if {@code id} is null
   */
  Client client();

  /**
   * Creates a transport server.
   *
   * @return The transport server.
   * @throws NullPointerException if {@code id} is null
   */
  Server server();

}
