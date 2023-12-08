package bio.terra.service.dataset.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
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
public class UnlockDatasetStepTest {

  @Mock private DatasetService datasetService;
  @Mock private FlightContext flightContext;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String FLIGHT_ID = "flight-id";
  private static final List<Exception> RETRYABLE_EXCEPTIONS =
      List.of(new RetryQueryException("Retry"), new TransactionSystemException("Retry"));
  private UnlockDatasetStep step;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepWithIdFromStepConstruction(boolean sharedLock) {
    step = new UnlockDatasetStep(datasetService, DATASET_ID, sharedLock);
    mockFlightContextFlightId();

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(datasetService).unlock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepWithIdFromWorkingMap(boolean sharedLock) {
    step = new UnlockDatasetStep(datasetService, sharedLock);
    mockFlightContextFlightId();
    FlightMap workingMap = mockFlightContextWorkingMap();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, DATASET_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(datasetService).unlock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepFailsWhenNoIdSpecified(boolean sharedLock) {
    step = new UnlockDatasetStep(datasetService, sharedLock);
    mockFlightContextWorkingMap();

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), instanceOf(DatasetLockException.class));
    verifyNoInteractions(datasetService);
  }

  @ParameterizedTest
  @MethodSource
  void testDoStepRetriesWhenUnlockThrowsRetryableException(
      boolean sharedLock, Exception retryableException) {
    step = new UnlockDatasetStep(datasetService, DATASET_ID, sharedLock);
    mockFlightContextFlightId();
    doThrow(retryableException).when(datasetService).unlock(DATASET_ID, FLIGHT_ID, sharedLock);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    verify(datasetService).unlock(DATASET_ID, FLIGHT_ID, sharedLock);
  }

  @ParameterizedTest
  @MethodSource
  void testDoStepWithProvidedLockNameAndThrowException(
      boolean sharedLock, boolean throwLockException, boolean successfulLock) {
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.IS_SHARED_LOCK, sharedLock);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    String lockName = "lock-name";
    step = new UnlockDatasetStep(datasetService, DATASET_ID, lockName, throwLockException);
    when(datasetService.unlock(DATASET_ID, lockName, sharedLock)).thenReturn(successfulLock);

    StepResult doResult = step.doStep(flightContext);
    if (successfulLock || (!successfulLock && !throwLockException)) {
      assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    } else {
      assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    }
    verify(datasetService).unlock(DATASET_ID, lockName, sharedLock);
  }

  private static Stream<Arguments> testDoStepWithProvidedLockNameAndThrowException() {
    List<Arguments> arguments = new ArrayList<>();
    for (boolean sharedLock : List.of(true, false)) {
      for (boolean throwLockException : List.of(true, false)) {
        for (boolean successfulLock : List.of(true, false)) {
          arguments.add(Arguments.arguments(sharedLock, throwLockException, successfulLock));
        }
      }
    }
    return arguments.stream();
  }

  private static Stream<Arguments> testDoStepRetriesWhenUnlockThrowsRetryableException() {
    List<Arguments> arguments = new ArrayList<>();
    for (boolean sharedLock : List.of(true, false)) {
      for (Exception retryableException : RETRYABLE_EXCEPTIONS) {
        arguments.add(Arguments.arguments(sharedLock, retryableException));
      }
    }
    return arguments.stream();
  }

  private void mockFlightContextFlightId() {
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  private FlightMap mockFlightContextWorkingMap() {
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    return workingMap;
  }
}
