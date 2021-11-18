package bio.terra.common.exception;

import static org.assertj.core.api.Assertions.assertThat;

import bio.terra.common.category.Unit;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DataRepoExceptionTest extends TestCase {

  public void testToStringWithMessages() {
    final DataRepoException exception =
        new TestDataRepoException("BOOM", Arrays.asList("foo", "bar"));
    assertThat(exception.toString())
        .isEqualTo(
            "bio.terra.common.exception.DataRepoExceptionTest$TestDataRepoException: BOOM Details: foo; bar");
  }

  public void testToStringWithNoMessages() {
    final DataRepoException exception = new TestDataRepoException("BOOM");
    assertThat(exception.toString())
        .isEqualTo("bio.terra.common.exception.DataRepoExceptionTest$TestDataRepoException: BOOM");
  }

  /** Test extension of abstract class to test printing of messages */
  private static class TestDataRepoException extends DataRepoException {
    TestDataRepoException(String message) {
      super(message);
    }

    TestDataRepoException(String message, List<String> errorDetails) {
      super(message, errorDetails);
    }
  }
}
