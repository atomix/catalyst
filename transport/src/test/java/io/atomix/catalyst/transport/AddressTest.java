package io.atomix.catalyst.transport;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

@Test
public class AddressTest {
  public void shouldConstructFromString() {
    Address address = new Address("localhost:5000");
    assertEquals(address.host(), "localhost");
    assertEquals(address.port(), 5000);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void shouldThrowOnMissingPort() {
    new Address("localhost:");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void shouldThrowOnInvalidPort() {
    new Address("localhost:foo");
  }
}
