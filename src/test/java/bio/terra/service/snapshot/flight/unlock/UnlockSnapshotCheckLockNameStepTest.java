package bio.terra.service.snapshot.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.ResourceLocks;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.common.ResourceLockConflict;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
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
  private static final String LOCK_NAME = "lock-name";
  private static final String OTHER_LOCK_NAME = "other-lock-name";

  private FlightMap workingMap;
  private UnlockSnapshotCheckLockNameStep step;
  private SnapshotSummaryModel snapshotSummaryModel;
  @Mock private FlightContext flightContext;
  @Mock private SnapshotService snapshotService;

  @BeforeEach
  void beforeEach() {
    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    step = new UnlockSnapshotCheckLockNameStep(snapshotService, SNAPSHOT_ID, LOCK_NAME);

    snapshotSummaryModel = new SnapshotSummaryModel();
    when(snapshotService.retrieveSnapshotSummary(SNAPSHOT_ID)).thenReturn(snapshotSummaryModel);
  }

  @Test
  void doStep_success() throws InterruptedException {
    snapshotSummaryModel.resourceLocks(new ResourceLocks().exclusive(LOCK_NAME));

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));
    assertThat(workingMap.get(DatasetWorkingMapKeys.IS_SHARED_LOCK, Boolean.class), equalTo(false));
  }

  private static Stream<Arguments> doStep_failure() {
    String noLocksMessage = "Datasnapshot %s is not locked.".formatted(SNAPSHOT_ID);

    String noMatchingLockMessage =
        """
            Datasnapshot %s has no lock named '%s'.
            Do you mean to remove one of these existing locks instead?
            """
            .formatted(SNAPSHOT_ID, LOCK_NAME);

    return Stream.of(
        arguments(new ResourceLocks(), new ResourceLockConflict(noLocksMessage)),
        arguments(
            new ResourceLocks().exclusive(OTHER_LOCK_NAME),
            new ResourceLockConflict(noMatchingLockMessage, List.of(OTHER_LOCK_NAME))));
  }

  @ParameterizedTest
  @MethodSource
  void doStep_failure(ResourceLocks existingResourceLocks, ResourceLockConflict expectedException)
      throws InterruptedException {
    snapshotSummaryModel.resourceLocks(existingResourceLocks);

    StepResult stepResult = step.doStep(flightContext);
    assertThat(stepResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));

    Optional<Exception> maybeException = stepResult.getException();
    assertTrue(maybeException.isPresent());
    Exception exception = maybeException.get();
    assertThat(exception, instanceOf(ResourceLockConflict.class));
    ResourceLockConflict resourceLockConflict = (ResourceLockConflict) exception;
    assertThat(resourceLockConflict.getMessage(), equalTo(expectedException.getMessage()));
    assertThat(resourceLockConflict.getCauses(), equalTo(expectedException.getCauses()));
  }
}
