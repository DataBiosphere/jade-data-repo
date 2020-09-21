package bio.terra.common.exception;

import bio.terra.common.category.Unit;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Category(Unit.class)
public class DataRepoExceptionTest extends TestCase {

    public void testToStringWithMessages() {
        final DataRepoException exception = new TestDataRepoException("BOOM", Arrays.asList("foo", "bar"));
        assertThat(exception.toString()).isEqualTo(
            "bio.terra.common.exception.DataRepoExceptionTest$TestDataRepoException: BOOM Details: foo; bar"
        );
    }

    public void testToStringWithNoMessages() {
        final DataRepoException exception = new TestDataRepoException("BOOM");
        assertThat(exception.toString()).isEqualTo(
            "bio.terra.common.exception.DataRepoExceptionTest$TestDataRepoException: BOOM"
        );
    }

    /**
     * Test extension of abstract class to test printing of messages
     */
    private static class TestDataRepoException extends DataRepoException {
        public TestDataRepoException(String message) {
            super(message);
        }

        public TestDataRepoException(String message, List<String> errorDetails) {
            super(message, errorDetails);
        }
    }
}
