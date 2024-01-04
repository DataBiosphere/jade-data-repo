package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import java.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class DateTimeUtilsTest {

  private static final String INSTANT_STRING = "2022-01-01T03:23:12.872120Z";
  private static final Instant INSTANT = Instant.parse(INSTANT_STRING);
  private static final Instant NANO_INSTANT = Instant.parse("2022-01-01T03:23:12.87212123Z");
  // Manually figured out the long representation of CREATED_AT for testing
  private static final long MICROS = 1641007392872120L;
  // When converted, an additional 1 is added at the end of the long value
  private static final long CONVERTED_NANOS = 1641007392872121L;

  @Test
  public void testToEpochMicros() {
    assertThat("can convert to micros", DateTimeUtils.toEpochMicros(INSTANT), equalTo(MICROS));
  }

  @Test
  public void testOfEpochMicros() {
    assertThat("can convert to micros", DateTimeUtils.ofEpicMicros(MICROS), equalTo(INSTANT));
  }

  @Test
  public void testNanoToEpochMicros() {
    assertThat(
        "Can convert nanos to micros",
        DateTimeUtils.toEpochMicros(NANO_INSTANT),
        equalTo(CONVERTED_NANOS));
  }

  @Test
  public void testMicrosToString() {
    assertThat(
        "to string method is correct",
        DateTimeUtils.toMicrosString(INSTANT),
        equalTo(INSTANT_STRING));
  }
}
