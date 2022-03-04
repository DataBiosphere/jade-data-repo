package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import java.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class DateTimeUtilsTest {

  private static final Instant INSTANT = Instant.parse("2022-01-01T03:23:12.87212Z");
  // Manually figured out the long representation of CREATED_AT for testing
  private static final long MICROS = 1641007392872120L;

  @Test
  public void testToEpochMicros() {
    assertThat("can convert to micros", DateTimeUtils.toEpochMicros(INSTANT), equalTo(MICROS));
  }

  @Test
  public void testOfEpochMicros() {
    assertThat("can convert to micros", DateTimeUtils.ofEpicMicros(MICROS), equalTo(INSTANT));
  }
}
