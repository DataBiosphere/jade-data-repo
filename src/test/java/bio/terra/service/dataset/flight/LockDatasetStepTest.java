package bio.terra.service.dataset.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.TransactionSystemException;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class LockDatasetStepTest {

  @Mock private DatasetService datasetService;
  @Mock private FlightContext flightContext;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String FLIGHT_ID = "flight-id";
  private static final DatasetNotFoundException DATASET_NOT_FOUND_EXCEPTION =
      new DatasetNotFoundException("Dataset not found");
  private static final List<Exception> RETRYABLE_EXCEPTIONS =
      List.of(
          new RetryQueryException("Retry"),
          new DatasetLockException("Dataset locked"),
          new TransactionSystemException("Transaction conflict"));
  private LockDatasetStep step;

  @BeforeEach
  void setup() {
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  @ParameterizedTest
  @MethodSource
  void testDoStep(boolean sharedLock, boolean suppressNotFoundException) {
    step = new LockDatasetStep(datasetService, DATASET_ID, sharedLock, suppressNotFoundException);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(datasetService).lock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  private static Stream<Arguments> testDoStep() {
    List<Arguments> arguments = new ArrayList<>();
    for (boolean sharedLock : List.of(true, false)) {
      for (boolean suppressNotFoundException : List.of(true, false)) {
        arguments.add(arguments(sharedLock, suppressNotFoundException));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource
  void testDoStepRetriesWhenLockThrowsRetryableException(
      boolean sharedLock, boolean suppressNotFoundException, Exception expectedException) {
    step = new LockDatasetStep(datasetService, DATASET_ID, sharedLock, suppressNotFoundException);
    doThrow(expectedException).when(datasetService).lock(DATASET_ID, FLIGHT_ID, sharedLock);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), equalTo(expectedException));

    verify(datasetService).lock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  private static Stream<Arguments> testDoStepRetriesWhenLockThrowsRetryableException() {
    List<Arguments> arguments = new ArrayList<>();
    for (boolean sharedLock : List.of(true, false)) {
      for (boolean suppressNotFoundException : List.of(true, false)) {
        for (Exception expectedException : RETRYABLE_EXCEPTIONS)
          arguments.add(arguments(sharedLock, suppressNotFoundException, expectedException));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepDatasetNotFoundSuppressed(boolean sharedLock) {
    step = new LockDatasetStep(datasetService, DATASET_ID, sharedLock, true);
    doThrow(DATASET_NOT_FOUND_EXCEPTION)
        .when(datasetService)
        .lock(DATASET_ID, FLIGHT_ID, sharedLock);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(datasetService).lock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepDatasetNotFoundUnsuppressed(boolean sharedLock) {
    step = new LockDatasetStep(datasetService, DATASET_ID, sharedLock, false);
    doThrow(DATASET_NOT_FOUND_EXCEPTION)
        .when(datasetService)
        .lock(DATASET_ID, FLIGHT_ID, sharedLock);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), equalTo(DATASET_NOT_FOUND_EXCEPTION));
    verify(datasetService).lock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUndoStep(boolean sharedLock) {
    step = new LockDatasetStep(datasetService, DATASET_ID, sharedLock, false);

    StepResult undoResult = step.undoStep(flightContext);

    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(datasetService).unlock(DATASET_ID, FLIGHT_ID, sharedLock);
  }
}
