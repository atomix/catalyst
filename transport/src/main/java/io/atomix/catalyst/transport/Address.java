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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

import java.net.InetSocketAddress;

/**
 * Network address.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Address implements CatalystSerializable {
  private String host;
  private int port;
  private InetSocketAddress address;

  public Address() {
  }

  public Address(String address) {
    Assert.notNull(address, "address");
    String[] components = address.split(":");
    Assert.arg(components.length == 2, "%s must contain address:port", address);

    this.host = components[0];
    try {
      this.port = Integer.parseInt(components[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(components[1] + " is not a number");
    }
    this.address = new InetSocketAddress(host, port);
  }

  public Address(Address address) {
    this(address.host, address.port, Assert.notNull(address, "address").address);
  }

  public Address(String host, int port) {
    this(host, port, new InetSocketAddress(host, port));
  }

  public Address(InetSocketAddress address) {
    this(address.getHostName(), address.getPort(), address);
  }

  private Address(String host, int port, InetSocketAddress address) {
    this.host = Assert.notNull(host, "host");
    this.port = port;
    this.address = Assert.notNull(address, "address");
  }

  /**
   * Returns the address host.
   *
   * @return The address host.
   */
  public String host() {
    return address.getHostString();
  }

  /**
   * Returns the address port.
   *
   * @return The address port.
   */
  public int port() {
    return address.getPort();
  }

  /**
   * Returns the underlying address.
   *
   * @return The underlying address.
   */
  public InetSocketAddress socketAddress() {
    return address;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeUTF8(host()).writeInt(port());
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    address = new InetSocketAddress(buffer.readUTF8(), buffer.readInt());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Address && ((Address) object).address.equals(address);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + address.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return address.toString();
  }

}
