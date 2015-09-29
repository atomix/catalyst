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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

/**
 * Local transport test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LocalTransportTest extends ConcurrentTestCase {

  /**
   * Tests connecting to a server and sending a message.
   */
  public void testSendReceive() throws Throwable {
    LocalServerRegistry registry = new LocalServerRegistry();

    Transport clientTransport = new LocalTransport(registry);

    Transport serverTransport = new LocalTransport(registry);

    Server server = serverTransport.server();
    Client client = clientTransport.client();

    ThreadContext context = new SingleThreadContext("test-thread-%d", new Serializer());

    context.executor().execute(() -> {
      try {
        server.listen(new Address(new InetSocketAddress(InetAddress.getByName("localhost"), 5555)), connection -> {
          connection.<String, String>handler(String.class, message -> {
            threadAssertEquals("Hello world!", message);
            return CompletableFuture.completedFuture("Hello world back!");
          });
        }).thenRun(this::resume);
      } catch (UnknownHostException e) {
        threadFail(e);
      }
    });
    await();

    context.executor().execute(() -> {
      try {
        client.connect(new Address(new InetSocketAddress(InetAddress.getByName("localhost"), 5555))).thenAccept(connection -> {
          connection.send("Hello world!").thenAccept(response -> {
            threadAssertEquals("Hello world back!", response);
            resume();
          });
        });
      } catch (UnknownHostException e) {
        threadFail(e);
      }
    });
    await();
  }

}
