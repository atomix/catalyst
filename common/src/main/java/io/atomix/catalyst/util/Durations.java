package io.atomix.catalyst.util;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility for creating {@link Duration} instances.
 *
 * @author Jonathan Halterman
 */
public final class Durations {
  private Durations() {
  }

  private static final Pattern PATTERN = Pattern.compile("(∞|inf|infinite)|" + "(([\\d]+)[\\s]*(" + "ns|nanosecond(s)?|"
      + "ms|millisecond(s)?|" + "s|second(s)?|" + "m|minute(s)?|" + "h|hour(s)?|" + "d|day(s)?" + ")?)");
  private static final Map<String, TimeUnit> SUFFIXES;

  static {
    SUFFIXES = new HashMap<String, TimeUnit>();

    SUFFIXES.put("ns", TimeUnit.NANOSECONDS);
    SUFFIXES.put("nanosecond", TimeUnit.NANOSECONDS);
    SUFFIXES.put("nanoseconds", TimeUnit.NANOSECONDS);

    SUFFIXES.put("ms", TimeUnit.MILLISECONDS);
    SUFFIXES.put("millisecond", TimeUnit.MILLISECONDS);
    SUFFIXES.put("milliseconds", TimeUnit.MILLISECONDS);

    SUFFIXES.put("s", TimeUnit.SECONDS);
    SUFFIXES.put("second", TimeUnit.SECONDS);
    SUFFIXES.put("seconds", TimeUnit.SECONDS);

    SUFFIXES.put("m", TimeUnit.MINUTES);
    SUFFIXES.put("minute", TimeUnit.MINUTES);
    SUFFIXES.put("minutes", TimeUnit.MINUTES);

    SUFFIXES.put("h", TimeUnit.HOURS);
    SUFFIXES.put("hour", TimeUnit.HOURS);
    SUFFIXES.put("hours", TimeUnit.HOURS);

    SUFFIXES.put("d", TimeUnit.DAYS);
    SUFFIXES.put("day", TimeUnit.DAYS);
    SUFFIXES.put("days", TimeUnit.DAYS);
  }

  /**
   * Returns a Duration from the parsed {@code duration}. Example:
   *
   * <pre>
   * 5
   * 5 s
   * 5 seconds
   * 10m
   * 10 minutes
   * </pre>
   *
   * <p>
   * If a unit is not given, the default unit will be milliseconds.
   */
  public static Duration of(String duration) {
    Matcher matcher = PATTERN.matcher(duration);
    Assert.arg(matcher.matches(), "Invalid duration: %s", duration);

    if (matcher.group(1) != null) {
      return Duration.ofSeconds(Long.MAX_VALUE);
    } else {
      String unit = matcher.group(4);
      String value = matcher.group(3);
      return Duration.ofNanos(TimeUnit.NANOSECONDS.convert(Long.parseLong(value),
          unit == null ? TimeUnit.MILLISECONDS : SUFFIXES.get(unit)));
    }
  }
}
