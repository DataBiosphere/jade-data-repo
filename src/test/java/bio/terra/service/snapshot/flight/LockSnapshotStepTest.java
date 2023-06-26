package bio.terra.service.snapshot.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotLockException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class LockSnapshotStepTest {

  @Mock private SnapshotDao snapshotDao;
  @Mock private FlightContext flightContext;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String FLIGHT_ID = "flight-id";
  private static final SnapshotNotFoundException SNAPSHOT_NOT_FOUND_EXCEPTION =
      new SnapshotNotFoundException("Snapshot not found");
  private LockSnapshotStep step;

  @BeforeEach
  void setup() {
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStep(boolean suppressNotFoundException) {
    step = new LockSnapshotStep(snapshotDao, SNAPSHOT_ID, suppressNotFoundException);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDoStepFailsWhenSnapshotLocked(boolean suppressNotFoundException) {
    step = new LockSnapshotStep(snapshotDao, SNAPSHOT_ID, suppressNotFoundException);
    var expectedException = new SnapshotLockException("Snapshot already locked");
    doThrow(expectedException).when(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), equalTo(expectedException));

    verify(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);
  }

  @Test
  void testDoStepSnapshotNotFoundSuppressed() {
    step = new LockSnapshotStep(snapshotDao, SNAPSHOT_ID, true);
    doThrow(SNAPSHOT_NOT_FOUND_EXCEPTION).when(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);
  }

  @Test
  void testDoStepSnapshotNotFoundUnsuppressed() {
    step = new LockSnapshotStep(snapshotDao, SNAPSHOT_ID, false);
    doThrow(SNAPSHOT_NOT_FOUND_EXCEPTION).when(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), equalTo(SNAPSHOT_NOT_FOUND_EXCEPTION));
    verify(snapshotDao).lock(SNAPSHOT_ID, FLIGHT_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUndoStep(boolean suppressNotFoundException) {
    step = new LockSnapshotStep(snapshotDao, SNAPSHOT_ID, suppressNotFoundException);

    StepResult undoResult = step.undoStep(flightContext);

    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).unlock(SNAPSHOT_ID, FLIGHT_ID);
  }
}
