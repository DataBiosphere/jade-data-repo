package bio.terra.service.dataset.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ResourceLocks;
import bio.terra.service.dataset.DatasetService;
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
class UnlockDatasetCheckLockNameStepTest {
  private static final UUID DATASET_ID = UUID.randomUUID();
  private UnlockDatasetCheckLockNameStep step;
  @Mock private FlightContext flightContext;
  @Mock private DatasetService datasetService;

  @ParameterizedTest
  @MethodSource
  void doStep(
      DatasetSummaryModel datasetSummaryModel,
      String requestedLockName,
      StepStatus expectedStatus,
      String exceptionMessage)
      throws InterruptedException {
    // Setup
    step = new UnlockDatasetCheckLockNameStep(datasetService, DATASET_ID, requestedLockName);
    when(datasetService.retrieveDatasetSummary(DATASET_ID)).thenReturn(datasetSummaryModel);

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
            new DatasetSummaryModel().resourceLocks(new ResourceLocks()),
            "lockName",
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            "Resource is not locked."),
        arguments(
            new DatasetSummaryModel().resourceLocks(new ResourceLocks().exclusive("LockName")),
            "LockName",
            StepStatus.STEP_RESULT_SUCCESS,
            null),
        arguments(
            new DatasetSummaryModel().resourceLocks(new ResourceLocks().exclusive("LockName")),
            "OtherLockName",
            StepStatus.STEP_RESULT_FAILURE_FATAL,
            "Resource is not locked by lock OtherLockName. Resource is locked by LockName."));
  }
}
