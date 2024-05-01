package bio.terra.service.snapshot.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class UnlockSnapshotStepTest {

  @Mock private SnapshotDao snapshotDao;
  @Mock private FlightContext flightContext;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String FLIGHT_ID = "flight-id";
  private UnlockSnapshotStep step;

  @Test
  void testDoStepWithIdFromStepConstruction() {
    step = new UnlockSnapshotStep(snapshotDao, SNAPSHOT_ID);
    mockFlightContextFlightId();

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).unlock(SNAPSHOT_ID, FLIGHT_ID);
  }

  @Test
  void testDoStepWithIdFromWorkingMap() {
    step = new UnlockSnapshotStep(snapshotDao, null);
    mockFlightContextFlightId();
    FlightMap workingMap = mockFlightContextWorkingMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, SNAPSHOT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).unlock(SNAPSHOT_ID, FLIGHT_ID);
  }

  @Test
  void testDoStepFailsWhenNoIdSpecified() {
    step = new UnlockSnapshotStep(snapshotDao, null);
    mockFlightContextWorkingMap();

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), instanceOf(SnapshotLockException.class));
    verifyNoInteractions(snapshotDao);
  }

  @Test
  void testDoStepWithProvidedLockName() {
    String lockName = "lock-name";
    step = new UnlockSnapshotStep(snapshotDao, SNAPSHOT_ID, lockName, true);
    when(snapshotDao.unlock(SNAPSHOT_ID, lockName)).thenReturn(true);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).unlock(SNAPSHOT_ID, lockName);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testThrowLockException(boolean throwLockException) {
    String lockName = "lock-name";
    step = new UnlockSnapshotStep(snapshotDao, SNAPSHOT_ID, lockName, throwLockException);
    // Mock the case where the lock Fails, so false is returned
    when(snapshotDao.unlock(SNAPSHOT_ID, lockName)).thenReturn(false);

    StepResult doResult = step.doStep(flightContext);

    if (throwLockException) {
      assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    } else {
      assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    }
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
