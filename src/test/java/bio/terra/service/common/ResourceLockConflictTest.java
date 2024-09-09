package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.ConflictException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class ResourceLockConflictTest {
  private static final String MESSAGE = "exception message";
  private static final List<String> CAUSES = List.of("cause1", "cause2");

  @Test
  void resourceLockConflict_message() {
    ResourceLockConflict exception = new ResourceLockConflict(MESSAGE);
    assertThat(exception, instanceOf(ConflictException.class));
    assertThat(exception.getMessage(), equalTo(MESSAGE));
    assertThat(exception.getCauses(), empty());
  }

  @Test
  void resourceLockConflict_messageAndCauses() {
    ResourceLockConflict exception = new ResourceLockConflict(MESSAGE, CAUSES);
    assertThat(exception, instanceOf(ConflictException.class));
    assertThat(exception.getMessage(), equalTo(MESSAGE));
    assertThat(exception.getCauses(), equalTo(CAUSES));
  }
}
