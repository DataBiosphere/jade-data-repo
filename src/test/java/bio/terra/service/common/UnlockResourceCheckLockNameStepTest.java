package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class UnlockResourceCheckLockNameStepTest {
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private UnlockResourceCheckLockNameStep step;
  @Mock private FlightContext flightContext;

  @ParameterizedTest
  @MethodSource
  void doStep(String requestedLockName, String actualLockName, StepStatus expectedStatus)
      throws InterruptedException {
    // Setup
    step =
        new UnlockResourceCheckLockNameStep(requestedLockName) {
          @Override
          protected String getExclusiveLock() {
            return actualLockName;
          }
        };

    // Perform Step
    var stepResult = step.doStep(flightContext);

    // Confirm response is correctly returned
    assertThat(stepResult.getStepStatus(), equalTo(expectedStatus));
  }

  private static Stream<Arguments> doStep() {
    return Stream.of(
        arguments("LockName", "LockName", StepStatus.STEP_RESULT_SUCCESS),
        arguments("LockName", "OtherLockName", StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments("LockName", null, StepStatus.STEP_RESULT_FAILURE_FATAL));
  }
}
