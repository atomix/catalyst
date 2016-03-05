package io.atomix.catalyst.util;

import static org.testng.Assert.*;

import java.time.Duration;

import org.testng.annotations.Test;

@Test
public class DurationsTest {
  public void testValidDurationStrings(){
    assertEquals(Durations.of("523"), Duration.ofMillis(523));
    assertEquals(Durations.of("5ns"), Duration.ofNanos(5));
    assertEquals(Durations.of("5milliseconds"), Duration.ofMillis(5));
    assertEquals(Durations.of("5 seconds"), Duration.ofSeconds(5));
    assertEquals(Durations.of("5 minutes"), Duration.ofMinutes(5));
    assertEquals(Durations.of("5 hours"), Duration.ofHours(5));
    assertEquals(Durations.of("5 days"), Duration.ofDays(5));
    assertEquals(Durations.of("inf"), Duration.ofSeconds(Long.MAX_VALUE));
    assertEquals(Durations.of("infinite"), Duration.ofSeconds(Long.MAX_VALUE));
    assertEquals(Durations.of("∞"), Duration.ofSeconds(Long.MAX_VALUE));
    assertEquals(Durations.of("0s"), Duration.ofSeconds(0));
  }

  private void testInvalidDurationString(String duration){
    try{
      Durations.of(duration);
      fail("Duration string '" + duration + "' should not parse correctly." );
    }
    catch(IllegalArgumentException iae){
      //Expected
    }
  }

  public void testInvalidDurationStrings(){
    testInvalidDurationString("foobar");
    testInvalidDurationString("ms3");
    testInvalidDurationString("34 lightyears");
    testInvalidDurationString("34 seconds a day");
    testInvalidDurationString("5 days a week");
    testInvalidDurationString("");
    testInvalidDurationString("ns");
  }
}