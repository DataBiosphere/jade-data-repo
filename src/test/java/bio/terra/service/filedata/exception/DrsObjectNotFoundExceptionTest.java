package bio.terra.service.filedata.exception;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DrsObjectNotFoundExceptionTest {
  private static final String MESSAGE = "exception message";

  @Test
  void drsObjectNotFoundException() {
    DrsObjectNotFoundException exception = new DrsObjectNotFoundException(MESSAGE);
    assertThat(exception, instanceOf(NotFoundException.class));
    assertThat(exception.getMessage(), equalTo(MESSAGE));
    assertThat(exception.getCauses(), empty());
  }
}
