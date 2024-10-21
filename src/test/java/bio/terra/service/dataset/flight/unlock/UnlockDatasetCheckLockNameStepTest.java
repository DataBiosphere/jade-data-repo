package bio.terra.service.dataset.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ResourceLocks;
import bio.terra.service.common.ResourceLockConflict;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
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
class UnlockDatasetCheckLockNameStepTest {
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String LOCK_NAME = "lock-name";
  private static final String OTHER_LOCK_NAME = "other-lock-name";
  private static final String YET_ANOTHER_LOCK_NAME = "yet-another-lock-name";

  private FlightMap workingMap;
  private UnlockDatasetCheckLockNameStep step;
  private DatasetSummaryModel datasetSummaryModel;
  @Mock private FlightContext flightContext;
  @Mock private DatasetService datasetService;

  @BeforeEach
  void beforeEach() {
    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    step = new UnlockDatasetCheckLockNameStep(datasetService, DATASET_ID, LOCK_NAME);

    datasetSummaryModel = new DatasetSummaryModel();
    when(datasetService.retrieveDatasetSummary(DATASET_ID)).thenReturn(datasetSummaryModel);
  }

  @Test
  void doStep_success_exclusiveLock() throws InterruptedException {
    datasetSummaryModel.resourceLocks(new ResourceLocks().exclusive(LOCK_NAME));

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));
    assertThat(workingMap.get(DatasetWorkingMapKeys.IS_SHARED_LOCK, Boolean.class), equalTo(false));
  }

  @Test
  void doStep_success_sharedLock() throws InterruptedException {
    datasetSummaryModel.resourceLocks(new ResourceLocks().addSharedItem(LOCK_NAME));

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));
    assertThat(workingMap.get(DatasetWorkingMapKeys.IS_SHARED_LOCK, Boolean.class), equalTo(true));
  }

  private static Stream<Arguments> doStep_failure() {
    String noLocksMessage = "Dataset %s is not locked.".formatted(DATASET_ID);

    String noMatchingLockMessage =
        """
            Dataset %s has no lock named '%s'.
            Do you mean to remove one of these existing locks instead?
            """
            .formatted(DATASET_ID, LOCK_NAME);

    return Stream.of(
        arguments(new ResourceLocks(), new ResourceLockConflict(noLocksMessage)),
        arguments(
            new ResourceLocks().exclusive(OTHER_LOCK_NAME).addSharedItem(YET_ANOTHER_LOCK_NAME),
            new ResourceLockConflict(
                noMatchingLockMessage, List.of(OTHER_LOCK_NAME, YET_ANOTHER_LOCK_NAME))));
  }

  @ParameterizedTest
  @MethodSource
  void doStep_failure(ResourceLocks existingResourceLocks, ResourceLockConflict expectedException)
      throws InterruptedException {
    datasetSummaryModel.resourceLocks(existingResourceLocks);

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
