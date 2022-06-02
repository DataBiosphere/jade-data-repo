package bio.terra.common;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class DateTimeUtils {
  private DateTimeUtils() {}

  /**
   * Converts this instant to the number of microseconds from the epoch of 1970-01-01T00:00:00Z.
   *
   * @param instant The instant object to convert
   * @return A long
   */
  public static long toEpochMicros(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  /**
   * Obtains an instance of Instant using microseconds from the epoch of 1970-01-01T00:00:00Z.
   *
   * @param epochMicros the number of microseconds from 1970-01-01T00:00:00Z
   * @return an instant
   */
  public static Instant ofEpicMicros(long epochMicros) {
    return Instant.EPOCH.plus(epochMicros, ChronoUnit.MICROS);
  }

  public static Instant ofEpicNanos(long epochMicros) {
    return Instant.EPOCH.plus(epochMicros, ChronoUnit.NANOS);
  }
}
