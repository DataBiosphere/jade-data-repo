package bio.terra.service.snapshot.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
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
class UnlockSnapshotCheckLockNameStepTest {
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private UnlockSnapshotCheckLockNameStep step;
  @Mock private FlightContext flightContext;
  @Mock private SnapshotService snapshotService;

  @ParameterizedTest
  @MethodSource
  void doStep(
      SnapshotSummaryModel snapshotSummaryModel,
      String requestedLockName,
      StepStatus expectedStatus,
      String exceptionMessage)
      throws InterruptedException {
    // Setup
    step = new UnlockSnapshotCheckLockNameStep(snapshotService, SNAPSHOT_ID, requestedLockName);
    when(snapshotService.retrieveSnapshotSummary(SNAPSHOT_ID)).thenReturn(snapshotSummaryModel);
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    // Perform Step
    var stepResult = step.doStep(flightContext);

    // Confirm response is correctly returned
    assertThat(stepResult.getStepStatus(), equalTo(expectedStatus));
    if (exceptionMessage != null) {
      assertThat(
          "Expected exception",
          stepResult.getException().get().getMessage(),
          containsString(exceptionMessage));
    }
  }

  private static Stream<Arguments> doStep() {
    return Stream.of(
        arguments(
            new SnapshotSummaryModel().resourceLocks(new ResourceLocks()),
            "lockName",
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            "Resource is not locked."),
        arguments(
            new SnapshotSummaryModel().resourceLocks(new ResourceLocks().exclusive("LockName")),
            "LockName",
            StepStatus.STEP_RESULT_SUCCESS,
            null),
        arguments(
            new SnapshotSummaryModel().resourceLocks(new ResourceLocks().exclusive("LockName")),
            "OtherLockName",
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            "Resource not locked by OtherLockName. It is locked by flight(s) LockName."));
  }
}
