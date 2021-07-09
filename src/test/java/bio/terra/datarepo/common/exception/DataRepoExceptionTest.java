package bio.terra.datarepo.common.exception;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;

import bio.terra.datarepo.common.category.Unit;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class DataRepoExceptionTest extends TestCase {

  public void testToStringWithMessages() {
    var exception = new TestDataRepoException("BOOM", Arrays.asList("foo", "bar"));
    assertThat(exception.toString(), endsWith("TestDataRepoException: BOOM Details: foo; bar"));
  }

  public void testToStringWithNoMessages() {
    var exception = new TestDataRepoException("BOOM");
    assertThat(exception.toString(), endsWith("TestDataRepoException: BOOM"));
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
