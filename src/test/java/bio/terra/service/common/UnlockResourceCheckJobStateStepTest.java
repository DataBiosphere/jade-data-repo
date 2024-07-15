package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.job.JobService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightStatus;
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
class UnlockResourceCheckJobStateStepTest {
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private UnlockResourceCheckJobStateStep step;
  @Mock private JobService jobService;
  @Mock private FlightContext flightContext;

  @ParameterizedTest
  @MethodSource
  void doStep(FlightStatus flightStatus, StepStatus stepStatus) throws InterruptedException {
    // Setup
    step = new UnlockResourceCheckJobStateStep(jobService, RESOURCE_ID.toString());
    when(jobService.unauthRetrieveJobState(RESOURCE_ID.toString())).thenReturn(flightStatus);

    // Perform Step
    var stepResult = step.doStep(flightContext);

    // Confirm response is correctly returned
    assertThat(stepResult.getStepStatus(), equalTo(stepStatus));
  }

  private static Stream<Arguments> doStep() {
    return Stream.of(
        arguments(FlightStatus.RUNNING, StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments(FlightStatus.WAITING, StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments(FlightStatus.READY, StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments(FlightStatus.QUEUED, StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments(FlightStatus.READY_TO_RESTART, StepStatus.STEP_RESULT_FAILURE_FATAL),
        arguments(FlightStatus.ERROR, StepStatus.STEP_RESULT_SUCCESS),
        arguments(FlightStatus.FATAL, StepStatus.STEP_RESULT_SUCCESS),
        arguments(FlightStatus.SUCCESS, StepStatus.STEP_RESULT_SUCCESS));
  }
}
