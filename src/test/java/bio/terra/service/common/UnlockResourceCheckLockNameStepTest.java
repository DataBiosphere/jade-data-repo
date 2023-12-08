package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepStatus;
import java.util.List;
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
  private UnlockResourceCheckLockNameStep step;
  @Mock private FlightContext flightContext;

  @ParameterizedTest
  @MethodSource
  void doStep(String requestedLockName, List<String> actualLockNames, StepStatus expectedStatus)
      throws InterruptedException {
    // Setup
    if (expectedStatus.equals(StepStatus.STEP_RESULT_SUCCESS)) {
      var flightMap = new FlightMap();
      when(flightContext.getWorkingMap()).thenReturn(flightMap);
    }
    step =
        new UnlockResourceCheckLockNameStep(requestedLockName) {
          @Override
          protected List<String> getLocks() {
            return actualLockNames;
          }

          @Override
          protected boolean isSharedLock(String lockName) {
            return false;
          }
        };

    // Perform Step
    var stepResult = step.doStep(flightContext);

    // Confirm response is correctly returned
    assertThat(stepResult.getStepStatus(), equalTo(expectedStatus));
  }

  private static Stream<Arguments> doStep() {
    return Stream.of(
        arguments("LockName", List.of("LockName", "OtherLockName"), StepStatus.STEP_RESULT_SUCCESS),
        arguments("LockName", List.of("OtherLockName"), StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments("LockName", List.of(), StepStatus.STEP_RESULT_FAILURE_FATAL));
  }
}
