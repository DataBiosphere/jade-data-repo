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
    Instant microInstant = instant.truncatedTo(ChronoUnit.MICROS);
    return ChronoUnit.MICROS.between(Instant.EPOCH, microInstant);
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

  /** Truncates Instant to Microseconds and converts to string */
  public static String toMicrosString(Instant instant) {
    return instant.truncatedTo(ChronoUnit.MICROS).toString();
  }
}
