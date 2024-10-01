package bio.terra.service.filedata.exception;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DrsObjectNotFoundExceptionTest {
  private static final String MESSAGE = "exception message";
  private static final List<String> CAUSES = List.of("cause1", "cause2");

  @Test
  void drsObjectNotFoundException_message() {
    DrsObjectNotFoundException exception = new DrsObjectNotFoundException(MESSAGE);
    assertThat(exception, instanceOf(NotFoundException.class));
    assertThat(exception.getMessage(), equalTo(MESSAGE));
    assertThat(exception.getCauses(), empty());
  }

  @Test
  void drsObjectNotFoundException_messageAndCauses() {
    DrsObjectNotFoundException exception = new DrsObjectNotFoundException(MESSAGE, CAUSES);
    assertThat(exception, instanceOf(NotFoundException.class));
    assertThat(exception.getMessage(), equalTo(MESSAGE));
    assertThat(exception.getCauses(), equalTo(CAUSES));
  }
}
